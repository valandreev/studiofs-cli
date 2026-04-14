param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("discover", "prepare", "begin-read", "finish", "status", "report", "renew")]
    [string]$Action,

    [string]$MountPoint = "Z:",
    [string]$StatePath,
    [string]$StudioFsPath,
    [string]$ClientName = "codex-aja-live",
    [string]$CaseTitle = "AJA cache performance investigation",
    [string]$ProblemStatement = "Investigate low write speed and low cached read speed in AJA System Test",
    [string]$TargetEnvironment = $env:COMPUTERNAME,
    [string]$CaseWorkloadIdentity = "aja",
    [string]$RunWorkloadIdentity,
    [string]$CaseId,
    [string]$Actor = $env:USERNAME,
    [string]$Note = "AJA read phase complete",
    [string]$ReportId,
    [int]$ProfileDurationMs = 30000,
    [int]$ProfileSampleRateHz = 80,
    [switch]$PrintReport
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
if (-not $StudioFsPath) {
    $debugPath = Join-Path $repoRoot "target\debug\studiofs.exe"
    $releasePath = Join-Path $repoRoot "target\release\studiofs.exe"
    if (Test-Path -LiteralPath $debugPath) {
        $StudioFsPath = $debugPath
    }
    elseif (Test-Path -LiteralPath $releasePath) {
        $StudioFsPath = $releasePath
    }
    else {
        throw "Could not find studiofs.exe under target\debug or target\release."
    }
}

if (-not $StatePath) {
    $safeMount = ($MountPoint -replace '[:\\/ ]', '_')
    $StatePath = Join-Path $env:TEMP "studiofs-aja-investigation-$safeMount.json"
}

if (-not $RunWorkloadIdentity) {
    $RunWorkloadIdentity = "aja-{0}" -f (Get-Date -Format "yyyyMMdd-HHmmss")
}

function Set-StateField {
    param(
        [Parameter(Mandatory = $true)]
        [psobject]$State,
        [Parameter(Mandatory = $true)]
        [string]$Name,
        $Value
    )

    $State | Add-Member -NotePropertyName $Name -NotePropertyValue $Value -Force
}

function New-State {
    $state = [pscustomobject]@{}
    Set-StateField $state "mount_point" $MountPoint
    Set-StateField $state "state_path" $StatePath
    Set-StateField $state "studiofs_path" $StudioFsPath
    Set-StateField $state "client_name" $ClientName
    Set-StateField $state "lease_id" $null
    Set-StateField $state "case_id" $null
    Set-StateField $state "run_id" $null
    Set-StateField $state "profile_window_id" $null
    Set-StateField $state "report_id" $null
    Set-StateField $state "read_start_marker_title" "AJA read phase start"
    Set-StateField $state "read_stop_marker_title" "AJA read phase stop"
    return $state
}

function Save-State {
    param([Parameter(Mandatory = $true)][psobject]$State)
    $State | ConvertTo-Json -Depth 100 | Set-Content -LiteralPath $StatePath -Encoding utf8
}

function Load-State {
    if (-not (Test-Path -LiteralPath $StatePath)) {
        throw "State file not found: $StatePath"
    }

    return Get-Content -LiteralPath $StatePath -Raw | ConvertFrom-Json
}

function Invoke-StudioFsJson {
    param([Parameter(Mandatory = $true)][string[]]$Args)

    $output = & $StudioFsPath @Args
    $exitCode = $LASTEXITCODE
    if ($exitCode -ne 0) {
        throw "studiofs command failed with exit code ${exitCode}: $StudioFsPath $($Args -join ' ')"
    }

    $text = ($output | Out-String).Trim()
    if ([string]::IsNullOrWhiteSpace($text)) {
        throw "studiofs command returned no JSON output: $StudioFsPath $($Args -join ' ')"
    }

    try {
        return $text | ConvertFrom-Json
    }
    catch {
        throw "Failed to parse JSON output from: $StudioFsPath $($Args -join ' ')`nRaw output:`n$text"
    }
}

function Ensure-ControllerLease {
    param([Parameter(Mandatory = $true)][psobject]$State)

    $attach = $null
    $leaseId = $null
    try {
        $leaseId = $State.lease_id
    }
    catch {
        $leaseId = $null
    }

    if (-not [string]::IsNullOrWhiteSpace($leaseId)) {
        try {
            $attach = Invoke-StudioFsJson @(
                "internal", "diagnostics-attach", $State.mount_point,
                "--renew-lease-id", $leaseId,
                "--ttl-ms", "60000"
            )
        }
        catch {
            $attach = $null
        }
    }

    if ($null -eq $attach) {
        $attach = Invoke-StudioFsJson @(
            "internal", "diagnostics-attach", $State.mount_point,
            "--controller-client", $State.client_name,
            "--ttl-ms", "60000"
        )
    }

    if ($null -eq $attach.controller_lease) {
        throw "Failed to acquire a controller lease for $($State.mount_point)."
    }

    Set-StateField $State "lease_id" $attach.controller_lease.lease_id
    Set-StateField $State "lease_expires_unix_ms" $attach.controller_lease.expires_unix_ms
    Set-StateField $State "session" $attach.session
    Set-StateField $State "last_attach" $attach
    Save-State $State
    return $attach
}

function Show-PrepareSummary {
    param([Parameter(Mandatory = $true)][psobject]$State)

    Write-Host ""
    Write-Host "AJA live investigation is ready."
    Write-Host "Mount point     : $($State.mount_point)"
    Write-Host "Lease id        : $($State.lease_id)"
    Write-Host "Case id         : $($State.case_id)"
    Write-Host "Run id          : $($State.run_id)"
    Write-Host "State file      : $StatePath"
    Write-Host ""
    Write-Host "Next:"
    Write-Host "1. Start AJA System Test against $($State.mount_point)."
    Write-Host "2. Let the write phase complete and wait until the read phase is actively running."
    Write-Host "3. As soon as the read phase is clearly underway, run:"
    Write-Host "   powershell -ExecutionPolicy Bypass -File .\tools\aja-live-investigation.ps1 -Action begin-read -MountPoint $($State.mount_point)"
    Write-Host "4. Keep AJA running for at least $ProfileDurationMs ms after begin-read so upload contention has time to show up."
    Write-Host "5. When you are ready to stop AJA, run:"
    Write-Host "   powershell -ExecutionPolicy Bypass -File .\tools\aja-live-investigation.ps1 -Action finish -MountPoint $($State.mount_point) -PrintReport"
    Write-Host ""
}

switch ($Action) {
    "discover" {
        $sessions = Invoke-StudioFsJson @("internal", "diagnostics-discover")
        $matching = @($sessions | Where-Object { $_.mount_point -eq $MountPoint })
        if ($matching.Count -eq 0) {
            Write-Host "No live diagnostics session found for $MountPoint."
            exit 0
        }

        $matching | ConvertTo-Json -Depth 100
    }

    "prepare" {
        $state = New-State
        $null = Ensure-ControllerLease $state

        $case = $null
        if ([string]::IsNullOrWhiteSpace($CaseId)) {
            $case = Invoke-StudioFsJson @(
                "internal", "diagnostics-create-case", $MountPoint,
                "--lease-id", $state.lease_id,
                "--title", $CaseTitle,
                "--problem-statement", $ProblemStatement,
                "--workload-identity", $CaseWorkloadIdentity,
                "--target-environment", $TargetEnvironment
            )
            Set-StateField $state "case_id" $case.id
        }
        else {
            Set-StateField $state "case_id" $CaseId
        }

        $run = Invoke-StudioFsJson @(
            "internal", "diagnostics-create-run", $MountPoint,
            "--lease-id", $state.lease_id,
            "--case-id", $state.case_id,
            "--workload-identity", $RunWorkloadIdentity
        )
        Set-StateField $state "run_id" $run.id

        $profilingStatus = Invoke-StudioFsJson @(
            "internal", "diagnostics-profiling-status", $MountPoint
        )
        Set-StateField $state "profiling_status" $profilingStatus

        Save-State $state
        Show-PrepareSummary -State $state
    }

    "begin-read" {
        $state = Load-State
        $null = Ensure-ControllerLease $state

        $entry = Invoke-StudioFsJson @(
            "internal", "diagnostics-add-timeline", $state.mount_point,
            "--lease-id", $state.lease_id,
            "--run-id", $state.run_id,
            "--source", "cli",
            "--kind", "marker",
            "--title", $state.read_start_marker_title,
            "--actor", $Actor,
            "--reason", "read phase became active while uploads may still be draining"
        )
        Set-StateField $state "read_start_entry" $entry

        $window = Invoke-StudioFsJson @(
            "internal", "diagnostics-start-profiling", $state.mount_point,
            "--lease-id", $state.lease_id,
            "--run-id", $state.run_id,
            "--capture-mode", "wall-blocking",
            "--preset", "balanced",
            "--duration-ms", $ProfileDurationMs.ToString(),
            "--sample-rate-hz", $ProfileSampleRateHz.ToString()
        )
        Set-StateField $state "profile_window_id" $window.window_id
        Set-StateField $state "profile_window" $window
        Save-State $state

        Write-Host ""
        Write-Host "Read-phase marker recorded and profiling window started."
        Write-Host "Run id        : $($state.run_id)"
        Write-Host "Window id     : $($state.profile_window_id)"
        Write-Host "Window status : $($window.status)"
        Write-Host ""
        Write-Host "Keep AJA reading for at least $ProfileDurationMs ms before stopping it."
        Write-Host "When you are ready to stop AJA, run:"
        Write-Host "powershell -ExecutionPolicy Bypass -File .\tools\aja-live-investigation.ps1 -Action finish -MountPoint $($state.mount_point) -PrintReport"
        Write-Host ""
    }

    "renew" {
        $state = Load-State
        $attach = Ensure-ControllerLease $state
        Write-Host "Lease renewed for $($state.mount_point)."
        Write-Host "Lease id     : $($attach.controller_lease.lease_id)"
        Write-Host "Expires (ms) : $($attach.controller_lease.expires_unix_ms)"
    }

    "status" {
        $state = Load-State
        $null = Ensure-ControllerLease $state

        $profilingStatus = Invoke-StudioFsJson @(
            "internal", "diagnostics-profiling-status", $state.mount_point
        )
        $run = Invoke-StudioFsJson @("internal", "diagnostics-read-run", "--run-id", $state.run_id)

        [pscustomobject]@{
            mount_point      = $state.mount_point
            lease_id         = $state.lease_id
            case_id          = $state.case_id
            run_id           = $state.run_id
            profile_window_id = $state.profile_window_id
            active_window    = $profilingStatus.active_window
            report_ids       = $run.report_ids
            profiling_windows = $run.profiling_windows.Count
            timeline_entries = $run.timeline.Count
            state_path       = $StatePath
        } | ConvertTo-Json -Depth 100
    }

    "finish" {
        $state = Load-State
        $null = Ensure-ControllerLease $state

        $entry = Invoke-StudioFsJson @(
            "internal", "diagnostics-add-timeline", $state.mount_point,
            "--lease-id", $state.lease_id,
            "--run-id", $state.run_id,
            "--source", "cli",
            "--kind", "marker",
            "--title", $state.read_stop_marker_title,
            "--actor", $Actor,
            "--body", $Note,
            "--reason", "manual stop after sustained cached read window"
        )
        Set-StateField $state "read_stop_entry" $entry

        $profilingStatus = Invoke-StudioFsJson @(
            "internal", "diagnostics-profiling-status", $state.mount_point
        )

        $activeWindow = $profilingStatus.active_window
        if ($null -ne $activeWindow) {
            $windowToStop = $activeWindow.window_id
            $stoppedWindow = Invoke-StudioFsJson @(
                "internal", "diagnostics-stop-profiling", $state.mount_point,
                "--lease-id", $state.lease_id,
                "--window-id", $windowToStop
            )
            Set-StateField $state "stopped_profile_window" $stoppedWindow
        }

        $report = Invoke-StudioFsJson @(
            "internal", "diagnostics-materialize-report", $state.mount_point,
            "--lease-id", $state.lease_id,
            "--run-id", $state.run_id
        )
        Set-StateField $state "report_id" $report.id

        $run = Invoke-StudioFsJson @("internal", "diagnostics-read-run", "--run-id", $state.run_id)
        if ($run.report_ids.Count -gt 0) {
            $latestReportId = $run.report_ids[$run.report_ids.Count - 1]
            Set-StateField $state "report_id" $latestReportId
        }

        Save-State $state

        Write-Host ""
        Write-Host "AJA investigation run is complete."
        Write-Host "Run id         : $($state.run_id)"
        Write-Host "Profile window : $($state.profile_window_id)"
        Write-Host "Report id      : $($state.report_id)"
        Write-Host "State file     : $StatePath"
        Write-Host ""

        if ($PrintReport -and -not [string]::IsNullOrWhiteSpace($state.report_id)) {
            Invoke-StudioFsJson @("internal", "diagnostics-read-report", "--report-id", $state.report_id) | ConvertTo-Json -Depth 100
        }
    }

    "report" {
        $state = $null
        if (Test-Path -LiteralPath $StatePath) {
            $state = Load-State
        }

        $resolvedReportId = $ReportId
        if ([string]::IsNullOrWhiteSpace($resolvedReportId) -and $null -ne $state) {
            try {
                $resolvedReportId = $state.report_id
            }
            catch {
                $resolvedReportId = $null
            }
        }

        if ([string]::IsNullOrWhiteSpace($resolvedReportId)) {
            throw "No report id available. Pass -ReportId or run -Action finish first."
        }

        Invoke-StudioFsJson @("internal", "diagnostics-read-report", "--report-id", $resolvedReportId) | ConvertTo-Json -Depth 100
    }
}
