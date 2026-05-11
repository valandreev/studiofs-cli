$ErrorActionPreference = 'Stop'

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
$processFilter = "Name = 'studiofs.exe' OR Name = 'studiofs-e2e.exe'"

Get-CimInstance Win32_Process -Filter $processFilter |
    Where-Object {
        $cmd = $_.CommandLine -as [string]
        # Runner-owned mount commands still include the sandbox root via --cache-dir;
        # avoid matching every user-owned "studiofs mount" process on this machine.
        $_.ProcessId -ne $selfPid -and
            ($cmd -match $rootPattern -or $cmd -match '__mounted-chaos-op')
    } |
    ForEach-Object {
        if ($cleanStale) {
            try {
                Stop-Process -Id $_.ProcessId -Force -ErrorAction Stop
                "stopped $($_.ProcessId) $($_.Name) $($_.CommandLine)"
            } catch {
                "failed to stop $($_.ProcessId) $($_.Name): $($_.Exception.Message)"
            }
        } else {
            "$($_.ProcessId) $($_.Name) $($_.CommandLine)"
        }
    }
