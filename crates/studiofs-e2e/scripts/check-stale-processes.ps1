$ErrorActionPreference = 'Stop'

$names = @('studiofs.exe', 'studiofs-e2e.exe')
$selfPid = [int]$env:STUDIOFS_E2E_BASELINE_SELF_PID
$rootPattern = [regex]::Escape($env:STUDIOFS_E2E_BASELINE_ROOT)

Get-CimInstance Win32_Process |
    Where-Object {
        $cmd = $_.CommandLine -as [string]
        $names -contains $_.Name -and
            $_.ProcessId -ne $selfPid -and
            ($cmd -match $rootPattern -or $cmd -match '__mounted-chaos-op|\smount\s')
    } |
    ForEach-Object {
        "$($_.ProcessId) $($_.Name) $($_.CommandLine)"
    }
