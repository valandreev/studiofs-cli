$ErrorActionPreference = 'Stop'

$pathsJson = $env:STUDIOFS_E2E_OWNERSHIP_PATHS_JSON
if ([string]::IsNullOrWhiteSpace($pathsJson)) {
    Write-Output '[]'
    exit 0
}

$decodedPaths = $pathsJson | ConvertFrom-Json
$paths = @()
foreach ($path in $decodedPaths) {
    $paths += [string]$path
}
$reports = foreach ($path in $paths) {
    $pathText = [string]$path
    try {
        $acl = Get-Acl -LiteralPath $pathText -ErrorAction Stop
        [pscustomobject]@{
            path = $pathText
            owner = $acl.Owner
            error = $null
        }
    } catch {
        [pscustomobject]@{
            path = $pathText
            owner = $null
            error = $_.Exception.Message
        }
    }
}

$reports | ConvertTo-Json -Depth 4 -Compress
