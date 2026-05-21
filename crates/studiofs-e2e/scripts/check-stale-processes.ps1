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
$processFilter = "Name = 'studiofs.exe' OR Name = 'studiofs-e2e.exe' OR Name = 'studiofs-e2e-hotspot-helper.exe'"

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
                $taskkillPath = 'taskkill.exe'
                if (-not [string]::IsNullOrWhiteSpace($env:SystemRoot)) {
                    $systemTaskkill = Join-Path $env:SystemRoot 'System32\taskkill.exe'
                    if (Test-Path $systemTaskkill) {
                        $taskkillPath = $systemTaskkill
                    }
                }
                $taskkillStartInfo = [System.Diagnostics.ProcessStartInfo]::new()
                $taskkillStartInfo.FileName = $taskkillPath
                $taskkillStartInfo.UseShellExecute = $false
                $taskkillStartInfo.RedirectStandardOutput = $true
                $taskkillStartInfo.RedirectStandardError = $true
                $taskkillStartInfo.Arguments = "/PID $($_.ProcessId) /T /F"
                $taskkill = [System.Diagnostics.Process]::Start($taskkillStartInfo)
                if (-not $taskkill.WaitForExit(5000)) {
                    try {
                        $taskkill.Kill()
                    } catch {
                    }
                    throw "taskkill timed out after 5000 ms"
                }
                $exitCode = $taskkill.ExitCode
                if ($exitCode -ne 0 -and $exitCode -ne 128) {
                    $stdout = $taskkill.StandardOutput.ReadToEnd().Trim()
                    $stderr = $taskkill.StandardError.ReadToEnd().Trim()
                    throw "taskkill exited with code $exitCode; stdout=$stdout; stderr=$stderr"
                }
                "stopped $($_.ProcessId) $($_.Name) $($_.CommandLine)"
            } catch {
                "failed to stop $($_.ProcessId) $($_.Name): $($_.Exception.Message)"
            }
        } else {
            "$($_.ProcessId) $($_.Name) $($_.CommandLine)"
        }
    }
