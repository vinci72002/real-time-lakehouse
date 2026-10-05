# Planned restart. This does not run on normal submit.
#
#   .\scripts\savepoint.ps1 stop
#   .\scripts\savepoint.ps1 resume
#
# stop writes a savepoint and stops the job. The path is saved in
# savepoint-path.txt at the repo root. resume submits flink/flink_job.sql
# from that path.

param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("stop", "resume")]
    [string]$Action
)

$ErrorActionPreference = "Stop"
$JobManager = "flink-jobmanager"
$SavepointDir = "file:///opt/flink/data/savepoints"
$RepoRoot = Split-Path -Parent $PSScriptRoot
$PathFile = Join-Path $RepoRoot "savepoint-path.txt"

function Invoke-Flink {
    param([string[]]$DockerArgs)
    # docker writes JDK module warnings to stderr. PowerShell 5 treats that
    # as a terminating error when ErrorAction is Stop, even if the exit code is 0.
    $previous = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $output = & docker @DockerArgs 2>&1 | Out-String
        return @{ Output = $output; Code = $LASTEXITCODE }
    }
    finally {
        $ErrorActionPreference = $previous
    }
}

function Get-JobId {
    $result = Invoke-Flink @("exec", $JobManager, "./bin/flink", "list")
    $listing = $result.Output
    $match = [regex]::Match($listing, "([0-9a-f]{32}) : lakehouse-aircraft-telemetry")
    if (-not $match.Success) {
        throw "No running lakehouse-aircraft-telemetry job.`n$listing"
    }
    return $match.Groups[1].Value
}

if ($Action -eq "stop") {
    $jobId = Get-JobId
    Write-Host "Stopping $jobId with a savepoint..."
    $result = Invoke-Flink @(
        "exec", $JobManager, "./bin/flink", "stop", "-p", $SavepointDir, $jobId
    )
    $output = $result.Output
    Write-Host $output
    if ($result.Code -ne 0) {
        throw "flink stop failed with exit code $($result.Code)"
    }
    $path = [regex]::Match($output, "((?:file:)?/opt/flink/data/savepoints/savepoint-[^\s]+)")
    if (-not $path.Success) {
        throw "Savepoint path was not found in the stop output."
    }
    Set-Content -Path $PathFile -Value $path.Groups[1].Value.Trim() -Encoding ascii
    Write-Host "Saved $($path.Groups[1].Value)"
    return
}

if (-not (Test-Path $PathFile)) {
    throw "No savepoint-path.txt. Run .\scripts\savepoint.ps1 stop first."
}
$savepoint = (Get-Content $PathFile -Raw).Trim()
Write-Host "Restoring from $savepoint"

$sql = Get-Content (Join-Path $RepoRoot "flink\flink_job.sql") -Raw
$prefix = "SET 'execution.savepoint.path' = '$savepoint';`r`n"
$temp = Join-Path $RepoRoot "resume-from-savepoint.sql"
$utf8 = New-Object System.Text.UTF8Encoding $false
[System.IO.File]::WriteAllText($temp, $prefix + $sql, $utf8)
try {
    docker cp $temp "${JobManager}:/tmp/flink_job.sql"
    $result = Invoke-Flink @("exec", $JobManager, "./bin/sql-client.sh", "-f", "/tmp/flink_job.sql")
    Write-Host $result.Output
    if ($result.Code -ne 0) {
        throw "sql-client restore failed with exit code $($result.Code)"
    }
}
finally {
    Remove-Item $temp -ErrorAction SilentlyContinue
}

Write-Host "Done. Check Flink UI: http://localhost:8081"
