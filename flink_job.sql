-- =============================================================
-- Aircraft telemetry lakehouse (Flink SQL)
--
-- Kafka topic: aircraft-telemetry
--
-- Flow:
--   Kafka
--     → validate / tag data-quality issues
--     → first valid copy of each event_id
--         → aircraft_telemetry
--
-- Business anomaly:
--   engine_temp > 1000
--     → keep in main table
--     → overheat = TRUE
--
-- Data-quality / processing issues:
--   too_late / invalid / duplicate
--     → aircraft_telemetry_rejected
--
-- Storage:
--   Iceberg REST catalog + MinIO
--
-- Spark reads:
--   demo.aviation.aircraft_telemetry
--   demo.aviation.aircraft_telemetry_rejected
-- =============================================================


-- =============================================================
-- Submit
-- =============================================================

-- docker cp flink_job.sql flink-jobmanager:/tmp/flink_job.sql
--
-- docker exec flink-jobmanager \
--   ./bin/sql-client.sh -f /tmp/flink_job.sql
--
-- or:
-- .\submit_flink_job.ps1
--
-- Flink UI:
-- http://localhost:8081
--
-- Query main table:
-- docker exec spark-iceberg spark-sql -e \
-- "SELECT * FROM demo.aviation.aircraft_telemetry LIMIT 20"
--
-- Query rejected table:
-- docker exec spark-iceberg spark-sql -e \
-- "SELECT * FROM demo.aviation.aircraft_telemetry_rejected LIMIT 20"


-- =============================================================
-- 0. Job settings
-- =============================================================

SET 'pipeline.name' = 'lakehouse-aircraft-telemetry';

SET 'execution.runtime-mode' = 'streaming';

SET 'parallelism.default' = '1';


-- Iceberg commits a snapshot on a successful checkpoint.
SET 'execution.checkpointing.interval' = '30s';

SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';


-- State backend
SET 'state.backend.type' = 'rocksdb';

SET 'state.backend.incremental' = 'true';

SET 'state.checkpoints.dir'
    = 'file:///opt/flink/data/checkpoints/aircraft-telemetry';


-- Prevent an idle Kafka partition from holding back
-- the watermark forever.
SET 'table.exec.source.idle-timeout' = '10 s';


-- State used by deduplication / duplicate detection.
-- After one hour, an event_id can be treated as new.
SET 'table.exec.state.ttl' = '1 h';



-- =============================================================
-- 1. Kafka source
-- =============================================================

CREATE TEMPORARY TABLE kafka_telemetry (

    event_id       STRING,
    aircraft_id    STRING,
    `timestamp`    TIMESTAMP_LTZ(3),

    telemetry      ROW<
        altitude DOUBLE,
        speed DOUBLE,
        engine_temp DOUBLE
    >,

    -- Kafka metadata for traceability
    kafka_partition INT
        METADATA FROM 'partition' VIRTUAL,
    kafka_offset BIGINT
        METADATA FROM 'offset' VIRTUAL,
    kafka_timestamp TIMESTAMP_LTZ(3)
        METADATA FROM 'timestamp' VIRTUAL,


    -- Event-time field
    event_time AS `timestamp`,
    -- Processing time used by deduplication
    proc_time AS PROCTIME(),

    -- Allow five seconds of out-of-order data
    WATERMARK FOR event_time
        AS event_time - INTERVAL '5' SECOND

)
WITH (

    'connector' = 'kafka',

    'topic' = 'aircraft-telemetry',

    'properties.bootstrap.servers' = 'kafka:19092',

    'properties.group.id' = 'lakehouse-flink-telemetry',


    -- Resume from committed offsets.
    -- On the first run, start from latest.

    'scan.startup.mode' = 'group-offsets',

    'properties.auto.offset.reset' = 'latest',


    'format' = 'json',

    'json.timestamp-format.standard' = 'ISO-8601',

    'json.ignore-parse-errors' = 'true',

    'json.fail-on-missing-field' = 'false'

);



-- =============================================================
-- 2. Iceberg catalog
-- =============================================================

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



-- =============================================================
-- 2.1 Main Iceberg table
-- =============================================================

CREATE TABLE IF NOT EXISTS
lakehouse.aviation.aircraft_telemetry (

    event_id       STRING,
    aircraft_id    STRING,
    event_time     TIMESTAMP_LTZ(6),
    ingest_time    TIMESTAMP_LTZ(6),
    altitude       DOUBLE,
    speed          DOUBLE,
    engine_temp    DOUBLE,

    -- Business anomaly flag.
    -- High temperature is still valid telemetry.
    overheat       BOOLEAN,

    -- Event arrived behind the current watermark
    -- but was still within the accepted late window.
    late           BOOLEAN

)
PARTITIONED BY (aircraft_id)

WITH (

    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '20'

);



-- =============================================================
-- 2.2 Rejected Iceberg table
-- =============================================================

CREATE TABLE IF NOT EXISTS
lakehouse.aviation.aircraft_telemetry_rejected (

    event_id          STRING,
    aircraft_id       STRING,
    event_time        TIMESTAMP_LTZ(6),
    ingest_time       TIMESTAMP_LTZ(6),
    altitude          DOUBLE,
    speed             DOUBLE,
    engine_temp       DOUBLE,
    reason            STRING,


    -- Kafka metadata allows us to trace
    -- the rejected event back to the source.

    kafka_partition   INT,
    kafka_offset      BIGINT,
    kafka_timestamp   TIMESTAMP_LTZ(3)

)
PARTITIONED BY (aircraft_id)

WITH (

    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '20'
);



-- =============================================================
-- 3. Tag every incoming event
-- =============================================================

-- Lateness policy:
--
-- event_time >= watermark
--     → on time
--
-- watermark - 15s <= event_time < watermark
--     → late but accepted
--     → late = TRUE
--
-- event_time < watermark - 15s
--     → too late
--     → rejected
--
-- IMPORTANT:
--
-- engine_temp > 1000 is NOT a data-quality error.
-- It represents a business anomaly.
-- The event remains in the main table with:
--
--     overheat = TRUE


CREATE TEMPORARY VIEW telemetry_tagged AS

SELECT

    event_id,
    aircraft_id,
    event_time,
    proc_time,

    telemetry.altitude
        AS altitude,

    telemetry.speed
        AS speed,

    telemetry.engine_temp
        AS engine_temp,

    -- ---------------------------------------------------------
    -- Business anomaly
    -- ---------------------------------------------------------

    CASE

        WHEN telemetry.engine_temp IS NOT NULL
         AND telemetry.engine_temp > 1000

        THEN TRUE

        ELSE FALSE

    END AS overheat,


    -- ---------------------------------------------------------
    -- Kafka traceability
    -- ---------------------------------------------------------

    kafka_partition,
    kafka_offset,
    kafka_timestamp,


    -- ---------------------------------------------------------
    -- Late flag
    -- ---------------------------------------------------------

    COALESCE(

        event_time < CURRENT_WATERMARK(event_time),

        FALSE

    ) AS late,


    -- ---------------------------------------------------------
    -- Data-quality rejection reasons
    -- ---------------------------------------------------------

    NULLIF(

        CONCAT_WS(',',


            -- Missing / blank event ID

            CASE
                WHEN event_id IS NULL
                  OR TRIM(event_id) = ''
                THEN 'blank_event_id'
            END,

            -- Missing aircraft ID

            CASE
                WHEN aircraft_id IS NULL
                  OR TRIM(aircraft_id) = ''
                THEN 'missing_aircraft_id'
            END,


            -- Missing event time

            CASE
                WHEN event_time IS NULL
                THEN 'missing_event_time'
            END,


            -- Missing engine temperature

            CASE
                WHEN telemetry.engine_temp IS NULL
                THEN 'missing_engine_temp'
            END,


            -- Event is more than 15 seconds
            -- behind the current watermark.

            CASE
                WHEN CURRENT_WATERMARK(event_time) IS NOT NULL
                 AND event_time
                     <
                     CURRENT_WATERMARK(event_time)
                     - INTERVAL '15' SECOND
                THEN 'too_late'
            END
        ),

        ''
    ) AS reject_reason


FROM kafka_telemetry;



-- =============================================================
-- 4. Clean events
-- =============================================================

-- Only events without a data-quality rejection
-- continue into the clean stream.
--
-- NOTE:
-- overheat events are still valid and therefore remain here.


CREATE TEMPORARY VIEW telemetry_clean AS

SELECT

    event_id,
    aircraft_id,
    event_time,
    proc_time,
    altitude,
    speed,
    engine_temp,
    overheat,

    kafka_partition,
    kafka_offset,
    kafka_timestamp,

    late
FROM telemetry_tagged

WHERE reject_reason IS NULL;



-- =============================================================
-- 5. Detect duplicate events
-- =============================================================

-- The first event is written to the main table.
--
-- Second and later copies of the same event_id
-- are captured here and written to the rejected table.
--
-- State TTL = 1 hour.


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
        S.aircraft_id
            AS aircraft_id,

        S.event_time
            AS event_time,

        S.proc_time
            AS proc_time,

        S.altitude
            AS altitude,

        S.speed
            AS speed,

        S.engine_temp
            AS engine_temp,

        S.kafka_partition
            AS kafka_partition,

        S.kafka_offset
            AS kafka_offset,

        S.kafka_timestamp
            AS kafka_timestamp


    ONE ROW PER MATCH


    AFTER MATCH SKIP TO LAST S


    PATTERN (A S)

    DEFINE

        A AS TRUE,

        S AS TRUE

);



-- =============================================================
-- 6. Write main + rejected outputs
-- =============================================================

-- Both INSERT statements are executed as one Statement Set.
--
-- Main table:
--     valid events
--     first copy of event_id
--     overheat is retained as a business flag
--
-- Rejected table:
--     invalid
--     too late
--     duplicate


EXECUTE STATEMENT SET

BEGIN



-- =============================================================
-- 6.1 Main table
-- =============================================================

INSERT INTO lakehouse.aviation.aircraft_telemetry

SELECT

    event_id,
    aircraft_id,
    event_time,
    proc_time AS ingest_time,
    altitude,
    speed,
    engine_temp,
    overheat,
    late

FROM (

    SELECT

        *,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY proc_time ASC
        ) AS rn
    FROM telemetry_clean
)

WHERE rn = 1;



-- =============================================================
-- 6.2 Rejected table
-- =============================================================

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

    -- ---------------------------------------------------------
    -- Data-quality rejects
    -- ---------------------------------------------------------

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



    -- ---------------------------------------------------------
    -- Duplicate events
    -- ---------------------------------------------------------

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

-- List running Flink jobs:
--
-- docker exec flink-jobmanager ./bin/flink list


-- Stop with a savepoint:
--
-- docker exec flink-jobmanager \
-- ./bin/flink stop <job-id>


-- Compact Iceberg small files:
--
-- CALL demo.system.rewrite_data_files(
--     'aviation.aircraft_telemetry'
-- );


-- Expire old snapshots:
--
-- CALL demo.system.expire_snapshots(
--     'aviation.aircraft_telemetry',
--     TIMESTAMP '...'
-- );