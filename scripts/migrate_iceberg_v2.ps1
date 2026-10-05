# One-time development reset for the V2 telemetry schema.
#
# Drops the existing demo sink tables and their snapshots. The Flink job
# does not drop tables on startup. Run this only when you intend to discard
# the current aircraft_telemetry and aircraft_telemetry_rejected data, then
# submit flink/flink_job.sql so CREATE TABLE IF NOT EXISTS builds the V2 tables.
#
# The 1-minute aggregate table is created by the job. It is not dropped here
# unless it already exists from an earlier V2 submit.

$ErrorActionPreference = "Stop"

Write-Host "Dropping demo Iceberg tables (V2 reset)..."

docker exec spark-iceberg spark-sql -e "DROP TABLE IF EXISTS demo.aviation.aircraft_telemetry_1m"
docker exec spark-iceberg spark-sql -e "DROP TABLE IF EXISTS demo.aviation.aircraft_telemetry_rejected"
docker exec spark-iceberg spark-sql -e "DROP TABLE IF EXISTS demo.aviation.aircraft_telemetry"

Write-Host "Done. Submit .\scripts\submit_flink_job.ps1 next. This script does not start Flink."
