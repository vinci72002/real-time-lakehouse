# Flink container and SQL file
$JobManager = "flink-jobmanager"
$SqlFile = Join-Path $PSScriptRoot "flink_job.sql"

Write-Host "Submitting Flink SQL job..."

$running = docker exec $JobManager ./bin/flink list
if ($running -match "lakehouse-aircraft-telemetry") {
    Write-Host "A lakehouse-aircraft-telemetry job is already running."
    Write-Host "Cancel it explicitly before submitting again. This script does not cancel jobs or drop tables."
    Write-Host $running
    exit 1
}

# Copy SQL file into Flink JobManager
docker cp $SqlFile "${JobManager}:/tmp/flink_job.sql"

# Submit the Flink SQL job
docker exec $JobManager `
    ./bin/sql-client.sh `
    -f /tmp/flink_job.sql

Write-Host "Done. Check Flink UI: http://localhost:8081"