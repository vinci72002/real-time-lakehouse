<#
.SYNOPSIS
  Cancel the running lakehouse-aircraft-telemetry job and submit flink_job.sql.
  The previous job is cancelled without a savepoint, so operator state is not restored.
#>

$JobManager = 'flink-jobmanager'
$JobName = 'lakehouse-aircraft-telemetry'
$SqlFile = Join-Path $PSScriptRoot 'flink_job.sql'
$FlinkRest = 'http://localhost:8081'

# Cancel a job that is already running under this name.
$running = (Invoke-RestMethod "$FlinkRest/jobs/overview").jobs |
    Where-Object { $_.name -eq $JobName -and $_.state -eq 'RUNNING' }

foreach ($job in $running) {
    Write-Host "cancel $($job.jid)" -ForegroundColor Yellow
    docker exec $JobManager ./bin/flink cancel $job.jid | Out-Null
}

# Submit the SQL file. Streaming inserts return a Job ID and keep running.
docker cp $SqlFile "${JobManager}:/tmp/flink_job.sql" | Out-Null
$result = docker exec $JobManager ./bin/sql-client.sh -f /tmp/flink_job.sql 2>&1 | Out-String

$jobId = [regex]::Match($result, 'Job ID: (\w+)')
if (-not $jobId.Success) {
    Write-Host $result -ForegroundColor Red
    throw 'Flink SQL submit failed'
}
Write-Host "submitted: $($jobId.Groups[1].Value)  $FlinkRest/#/job/running" -ForegroundColor Green
