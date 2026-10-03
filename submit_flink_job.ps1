# Flink container and SQL file
$JobManager = "flink-jobmanager"
$SqlFile = Join-Path $PSScriptRoot "flink_job.sql"

Write-Host "Submitting Flink SQL job..."

# Copy SQL file into Flink JobManager
docker cp $SqlFile "${JobManager}:/tmp/flink_job.sql"

# Submit the Flink SQL job
docker exec $JobManager `
    ./bin/sql-client.sh `
    -f /tmp/flink_job.sql

Write-Host "Done. Check Flink UI: http://localhost:8081"