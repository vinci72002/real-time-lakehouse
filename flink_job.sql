-- =============================================================
-- Aircraft telemetry lakehouse (Flink SQL)
--   Kafka topic aircraft-telemetry
--   → tag a reject_reason
--   → first valid copy of each event_id  →  aircraft_telemetry
--   → overheat / too late / invalid / duplicate  →  aircraft_telemetry_rejected
--   Iceberg REST catalog + MinIO.
--   Spark reads the same tables as demo.aviation.*, not lakehouse.aviation.*.
-- =============================================================
-- Submit:
--   docker cp flink_job.sql flink-jobmanager:/tmp/flink_job.sql
--   docker exec flink-jobmanager ./bin/sql-client.sh -f /tmp/flink_job.sql
--   or:  .\submit_flink_job.ps1
--
-- Flink UI: http://localhost:8081
-- Query:
--   docker exec spark-iceberg spark-sql -e "SELECT * FROM demo.aviation.aircraft_telemetry LIMIT 20"
--   docker exec spark-iceberg spark-sql -e "SELECT * FROM demo.aviation.aircraft_telemetry_rejected LIMIT 20"
-- =============================================================


-- -------------------------------------------------------------
-- 0. Job settings
-- -------------------------------------------------------------
SET 'pipeline.name' = 'lakehouse-aircraft-telemetry';
SET 'execution.runtime-mode' = 'streaming';
SET 'parallelism.default' = '1';

-- Iceberg commits a snapshot only on a successful checkpoint.
-- The interval is the delay before a row is visible to readers.
SET 'execution.checkpointing.interval' = '30s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'state.backend.type' = 'rocksdb';
SET 'state.backend.incremental' = 'true';
SET 'state.checkpoints.dir' = 'file:///opt/flink/data/checkpoints/aircraft-telemetry';

-- An idle Kafka partition must not hold the watermark back forever.
SET 'table.exec.source.idle-timeout' = '10 s';
-- Dedup and duplicate-match state. After one hour an event_id is treated as new.
SET 'table.exec.state.ttl' = '1 h';


-- -------------------------------------------------------------
-- 1. Kafka source
-- -------------------------------------------------------------
-- timestamp looks like 2026-09-25T15:04:22.971Z. ISO-8601 parses to TIMESTAMP_LTZ
-- and does not depend on the session time zone (the container uses Asia/Shanghai).
CREATE TEMPORARY TABLE kafka_telemetry (
    event_id     STRING,
    aircraft_id  STRING,
    `timestamp`  TIMESTAMP_LTZ(3),
    telemetry    ROW<altitude DOUBLE, speed DOUBLE, engine_temp DOUBLE>,
    -- Kafka metadata, for the reject table
    kafka_partition INT
        METADATA FROM 'partition' VIRTUAL,
    kafka_offset BIGINT
        METADATA FROM 'offset' VIRTUAL,
    kafka_timestamp TIMESTAMP_LTZ(3)
        METADATA FROM 'timestamp' VIRTUAL,

    event_time   AS `timestamp`,
    proc_time    AS PROCTIME(),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'aircraft-telemetry',
    'properties.bootstrap.servers' = 'kafka:19092',
    'properties.group.id' = 'lakehouse-flink-telemetry',
    -- Resume from the committed offset. On the first start, begin at the latest offset.
    'scan.startup.mode' = 'group-offsets',
    'properties.auto.offset.reset' = 'latest',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'json.ignore-parse-errors' = 'true',
    'json.fail-on-missing-field' = 'false'
);


-- -------------------------------------------------------------
-- 2. Iceberg catalog and sink tables
-- -------------------------------------------------------------
-- Same REST catalog that spark-iceberg calls "demo".
-- Flink: lakehouse.aviation.aircraft_telemetry
-- Spark: demo.aviation.aircraft_telemetry
CREATE CATALOG lakehouse WITH (
    'type' = 'iceberg',
    'catalog-type' = 'rest',
    'uri' = 'http://iceberg-rest:8181',
    'warehouse' = 's3://warehouse/',
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint' = 'http://minio:9000',
    's3.path-style-access' = 'true',
    's3.access-key-id' = 'admin',
    's3.secret-access-key' = 'password',
    'client.region' = 'us-east-1'
);

CREATE DATABASE IF NOT EXISTS lakehouse.aviation;

CREATE TABLE IF NOT EXISTS lakehouse.aviation.aircraft_telemetry (
    event_id     STRING,
    aircraft_id  STRING,
    event_time   TIMESTAMP_LTZ(6),
    ingest_time  TIMESTAMP_LTZ(6),
    altitude     DOUBLE,
    speed        DOUBLE,
    engine_temp  DOUBLE,
    late         BOOLEAN
) PARTITIONED BY (aircraft_id)
WITH (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    -- A commit every 30s creates a lot of metadata files. Keep the last 20.
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '20'
);

-- Rows that did not make the main table. reason may list several causes.
-- This table is not deduplicated by event_id: a blank id would become one key
-- and hide every later invalid row.
CREATE TABLE IF NOT EXISTS lakehouse.aviation.aircraft_telemetry_rejected (
    event_id     STRING,
    aircraft_id  STRING,
    event_time   TIMESTAMP_LTZ(6),
    ingest_time  TIMESTAMP_LTZ(6),
    altitude     DOUBLE,
    speed        DOUBLE,
    engine_temp  DOUBLE,
    reason       STRING,
    -- Trace the row back to Kafka.
    kafka_partition INT,
    kafka_offset    BIGINT,
    kafka_timestamp TIMESTAMP_LTZ(3)
) PARTITIONED BY (aircraft_id)
WITH (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '20'
);


-- -------------------------------------------------------------
-- 3. Tag every row with a reason
-- -------------------------------------------------------------
-- Same three-way lateness rule as flink_job.py (watermark = max(event_time) - 5s):
--   event_time >= watermark                         on time
--   watermark - 15s <= event_time < watermark       late but kept, late = TRUE
--   event_time < watermark - 15s                    too late, reason contains too_late
-- Before the first watermark exists (NULL), nothing is marked too_late.
-- JSON that the connector cannot parse never reaches this view.
CREATE TEMPORARY VIEW telemetry_tagged AS
SELECT
    event_id,
    aircraft_id,
    event_time,
    proc_time,
    telemetry.altitude    AS altitude,
    telemetry.speed       AS speed,
    telemetry.engine_temp AS engine_temp,
    kafka_partition,
    kafka_offset,
    kafka_timestamp,
    COALESCE(event_time < CURRENT_WATERMARK(event_time), FALSE) AS late,
    NULLIF(CONCAT_WS(',',
        CASE WHEN event_id IS NULL OR TRIM(event_id) = '' THEN 'blank_event_id' END,
        CASE WHEN aircraft_id IS NULL OR TRIM(aircraft_id) = '' THEN 'missing_aircraft_id' END,
        CASE WHEN event_time IS NULL THEN 'missing_event_time' END,
        CASE WHEN telemetry.engine_temp IS NULL THEN 'missing_engine_temp' END,
        CASE WHEN telemetry.engine_temp > 1000 THEN 'overheat' END,
        -- Event time is older than the watermark by more than 15 seconds.
        CASE
            WHEN CURRENT_WATERMARK(event_time) IS NOT NULL
             AND event_time < CURRENT_WATERMARK(event_time) - INTERVAL '15' SECOND
            THEN 'too_late'
        END
    ), '') AS reject_reason
FROM kafka_telemetry;

CREATE TEMPORARY VIEW telemetry_clean AS
SELECT
    event_id,
    aircraft_id,
    event_time,
    proc_time,
    altitude,
    speed,
    engine_temp,
    kafka_partition,
    kafka_offset,
    kafka_timestamp,
    late
FROM telemetry_tagged
WHERE reject_reason IS NULL;

-- Second and later copies of an event_id. The match emits inserts only,
-- which Iceberg can append. The first copy is not emitted here; the dedup
-- insert below writes it to the main table. State TTL is the same 1 hour.
CREATE TEMPORARY VIEW telemetry_duplicate AS
SELECT
    event_id,
    aircraft_id,
    event_time,
    proc_time,
    altitude,
    speed,
    engine_temp,
    kafka_partition,
    kafka_offset,
    kafka_timestamp
FROM telemetry_clean
MATCH_RECOGNIZE (
    PARTITION BY event_id
    ORDER BY proc_time
    MEASURES
        S.aircraft_id     AS aircraft_id,
        S.event_time      AS event_time,
        S.proc_time       AS proc_time,
        S.altitude        AS altitude,
        S.speed           AS speed,
        S.engine_temp     AS engine_temp,
        S.kafka_partition AS kafka_partition,
        S.kafka_offset    AS kafka_offset,
        S.kafka_timestamp AS kafka_timestamp
    ONE ROW PER MATCH
    AFTER MATCH SKIP TO LAST S
    PATTERN (A S)
    DEFINE
        A AS TRUE,
        S AS TRUE
);


-- -------------------------------------------------------------
-- 4. Dedup into the main table, rejects into the other table
-- -------------------------------------------------------------
-- Both INSERT statements stay in one STATEMENT SET so Kafka is read once.
-- ROW_NUMBER with rn = 1 is planned as Deduplicate(keep first).
-- rn > 1 is not a bounded Top-N and does not compile, so duplicates use
-- the MATCH_RECOGNIZE view above.
EXECUTE STATEMENT SET
BEGIN
INSERT INTO lakehouse.aviation.aircraft_telemetry
SELECT
    event_id,
    aircraft_id,
    event_time,
    proc_time AS ingest_time,
    altitude,
    speed,
    engine_temp,
    late
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY proc_time ASC) AS rn
    FROM telemetry_clean
)
WHERE rn = 1;

INSERT INTO lakehouse.aviation.aircraft_telemetry_rejected
SELECT
    event_id,
    aircraft_id,
    event_time,
    proc_time AS ingest_time,
    altitude,
    speed,
    engine_temp,
    reason,
    kafka_partition,
    kafka_offset,
    kafka_timestamp
FROM (
    SELECT
        event_id,
        aircraft_id,
        event_time,
        proc_time,
        altitude,
        speed,
        engine_temp,
        reject_reason AS reason,
        kafka_partition,
        kafka_offset,
        kafka_timestamp
    FROM telemetry_tagged
    WHERE reject_reason IS NOT NULL
    UNION ALL
    SELECT
        event_id,
        aircraft_id,
        event_time,
        proc_time,
        altitude,
        speed,
        engine_temp,
        'duplicate' AS reason,
        kafka_partition,
        kafka_offset,
        kafka_timestamp
    FROM telemetry_duplicate
);
END;


-- =============================================================
-- Operations
-- =============================================================
-- Stop from the Flink UI, or:
--   docker exec flink-jobmanager ./bin/flink list
--   docker exec flink-jobmanager ./bin/flink stop <job-id>   -- stop with a savepoint
--
-- Compact small files in Spark:
--   CALL demo.system.rewrite_data_files('aviation.aircraft_telemetry');
--   CALL demo.system.expire_snapshots('aviation.aircraft_telemetry', TIMESTAMP '...');
