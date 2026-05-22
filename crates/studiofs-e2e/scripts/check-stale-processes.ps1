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
$candidateByPid = [ordered]@{}
$allowedProcessNames = @('studiofs', 'studiofs-e2e', 'studiofs-e2e-hotspot-helper')

function Add-CandidateProcess {
    param(
        [UInt32] $ProcessId,
        [string] $Source
    )

    if ($ProcessId -eq 0 -or $ProcessId -eq $selfPid -or $candidateByPid.Contains($ProcessId)) {
        return
    }

    $process = Get-Process -Id $ProcessId -ErrorAction SilentlyContinue
    if ($null -eq $process) {
        return
    }
    if ($allowedProcessNames -notcontains $process.ProcessName) {
        return
    }
    $candidateByPid[$ProcessId] = [pscustomobject]@{
        ProcessId = $ProcessId
        Name = $process.ProcessName
        Source = $Source
    }
}

function Stop-CandidateDirectly {
    param(
        [pscustomobject] $Candidate
    )

    try {
        $process = Get-Process -Id $Candidate.ProcessId -ErrorAction SilentlyContinue
        if ($null -eq $process) {
            return "stopped $($Candidate.ProcessId) $($Candidate.Name) $($Candidate.Source)"
        }
        $process.Kill()
        if (-not $process.WaitForExit(1000)) {
            return "failed to stop $($Candidate.ProcessId) $($Candidate.Name): direct kill timed out after 1000 ms"
        }
        return "stopped $($Candidate.ProcessId) $($Candidate.Name) $($Candidate.Source)"
    } catch {
        return "failed to stop $($Candidate.ProcessId) $($Candidate.Name): direct kill failed: $($_.Exception.Message)"
    }
}

$processMetadataPatterns = @(
    (Join-Path $normalizedRunnerRoot 'artifacts\clients\*\mount\mount-point.json'),
    (Join-Path $normalizedRunnerRoot '*\artifacts\clients\*\mount\mount-point.json'),
    (Join-Path $normalizedRunnerRoot 'artifacts\scenarios\*\hotspot-helper-processes\*.json'),
    (Join-Path $normalizedRunnerRoot '*\artifacts\scenarios\*\hotspot-helper-processes\*.json')
)

foreach ($metadataPattern in $processMetadataPatterns) {
    Get-ChildItem -Path $metadataPattern -File -ErrorAction SilentlyContinue |
        ForEach-Object {
            try {
                $metadata = Get-Content -LiteralPath $_.FullName -Raw | ConvertFrom-Json
                if ($null -ne $metadata.process_id) {
                    Add-CandidateProcess -ProcessId ([UInt32]$metadata.process_id) -Source $_.FullName
                }
            } catch {
            }
        }
}

Get-ChildItem -Path $normalizedRunnerRoot -Filter 'mount-point.json' -File -ErrorAction SilentlyContinue |
    ForEach-Object {
        try {
            $metadata = Get-Content -LiteralPath $_.FullName -Raw | ConvertFrom-Json
            if ($null -ne $metadata.process_id) {
                Add-CandidateProcess -ProcessId ([UInt32]$metadata.process_id) -Source $_.FullName
            }
        } catch {
        }
    }

$candidates = @($candidateByPid.Values)

if ($candidates.Count -eq 0) {
    return
}

if (-not $cleanStale) {
    foreach ($candidate in $candidates) {
        "$($candidate.ProcessId) $($candidate.Name) $($candidate.Source)"
    }
    return
}

$taskkillPath = 'taskkill.exe'
if (-not [string]::IsNullOrWhiteSpace($env:SystemRoot)) {
    $systemTaskkill = Join-Path $env:SystemRoot 'System32\taskkill.exe'
    if (Test-Path $systemTaskkill) {
        $taskkillPath = $systemTaskkill
    }
}

$taskkillArgs = @()
foreach ($candidate in $candidates) {
    $taskkillArgs += @('/PID', "$($candidate.ProcessId)")
}
$taskkillArgs += @('/T', '/F')

$taskkillStartInfo = [System.Diagnostics.ProcessStartInfo]::new()
$taskkillStartInfo.FileName = $taskkillPath
$taskkillStartInfo.UseShellExecute = $false
$taskkillStartInfo.RedirectStandardOutput = $true
$taskkillStartInfo.RedirectStandardError = $true
$taskkillStartInfo.Arguments = [string]::Join(' ', $taskkillArgs)
$taskkill = [System.Diagnostics.Process]::Start($taskkillStartInfo)
$stdoutTask = $taskkill.StandardOutput.ReadToEndAsync()
$stderrTask = $taskkill.StandardError.ReadToEndAsync()
if (-not $taskkill.WaitForExit(5000)) {
    try {
        $taskkill.Kill()
    } catch {
    }
    foreach ($candidate in $candidates) {
        $directResult = Stop-CandidateDirectly -Candidate $candidate
        if ($directResult.StartsWith('stopped ', [System.StringComparison]::Ordinal)) {
            $directResult
        } else {
            "failed to stop $($candidate.ProcessId) $($candidate.Name): batched taskkill timed out after 5000 ms; $directResult"
        }
    }
    return
}

$exitCode = $taskkill.ExitCode
$stdout = $stdoutTask.GetAwaiter().GetResult().Trim()
$stderr = $stderrTask.GetAwaiter().GetResult().Trim()
if ($exitCode -eq 0 -or $exitCode -eq 128) {
    foreach ($candidate in $candidates) {
        "stopped $($candidate.ProcessId) $($candidate.Name) $($candidate.Source)"
    }
} else {
    foreach ($candidate in $candidates) {
        $directResult = Stop-CandidateDirectly -Candidate $candidate
        if ($directResult.StartsWith('stopped ', [System.StringComparison]::Ordinal)) {
            $directResult
        } else {
            "failed to stop $($candidate.ProcessId) $($candidate.Name): taskkill exited with code $exitCode; stdout=$stdout; stderr=$stderr; $directResult"
        }
    }
}
