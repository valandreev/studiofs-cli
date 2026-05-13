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

$normalizedRunnerRoot = $runnerRoot.Replace('/', '\')
$rootPattern = [regex]::Escape($normalizedRunnerRoot)
$processFilter = "Name = 'studiofs.exe' OR Name = 'studiofs-e2e.exe'"

Get-CimInstance Win32_Process -Filter $processFilter |
    Where-Object {
        $cmd = $_.CommandLine -as [string]
        $normalizedCmd = $cmd.Replace('/', '\')
        # Runner-owned mount commands still include the sandbox root via --cache-dir;
        # avoid matching every user-owned "studiofs mount" process on this machine.
        $_.ProcessId -ne $selfPid -and
            ($normalizedCmd -match $rootPattern -or $cmd -match '__mounted-chaos-op')
    } |
    ForEach-Object {
        if ($cleanStale) {
            try {
                $taskkill = Start-Process -FilePath 'taskkill.exe' -ArgumentList @('/PID', "$($_.ProcessId)", '/T', '/F') -NoNewWindow -Wait -PassThru -ErrorAction Stop
                if ($taskkill.ExitCode -ne 0) {
                    throw "taskkill exited with code $($taskkill.ExitCode)"
                }
                "stopped $($_.ProcessId) $($_.Name) $($_.CommandLine)"
            } catch {
                "failed to stop $($_.ProcessId) $($_.Name): $($_.Exception.Message)"
            }
        } else {
            "$($_.ProcessId) $($_.Name) $($_.CommandLine)"
        }
    }
