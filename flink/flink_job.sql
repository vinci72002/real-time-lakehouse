-- =============================================================
-- Aircraft telemetry lakehouse (Flink SQL)
--
-- Kafka topic: aircraft-telemetry
-- Message schema: flat V2 telemetry (see src/telemetry_event.py)
--
-- Flow:
--   Kafka
--     → validate / tag data-quality issues
--     → first valid copy of each event_id
--         → aircraft_telemetry
--         → aircraft_telemetry_1m   (event-time 1-minute tumble)
--
-- Business anomaly:
--   engine_temp_c > 1000
--     → keep in the detail table
--     → overheat = TRUE
--
-- Data-quality / processing issues:
--   too_late / invalid / duplicate
--     → aircraft_telemetry_rejected
--
-- Timing, as implemented:
--
--   watermark = max event_time seen - 5 seconds
--
--   out-of-order
--     event_time is earlier than some event already seen,
--     and event_time >= watermark.
--     late = FALSE. The row is inside the 5-second out-of-orderness
--     bound, so the 1-minute window keeps it while that minute is open.
--
--   late
--     watermark - 15s <= event_time < watermark.
--     The detail table stores the row with late = TRUE.
--     Measured on Flink 1.19: the tumble still counts that row when
--     the window has not fired (watermark < window_end). late_count
--     is that subset. Being behind the watermark does not, by itself,
--     keep the row out of an open minute.
--
--   too_late
--     event_time < watermark - 15s.
--     Rejected. Absent from the detail table and from the tumble.
--
--   window closed
--     watermark >= window_end. The minute has been emitted.
--     A later event for that minute does not change the aggregate row,
--     even when the event is within 15 seconds and is stored on the
--     detail table with late = TRUE.
--
-- Storage:
--   Iceberg REST catalog + MinIO
--
-- Schema changes are not applied by this script. CREATE TABLE IF NOT
-- EXISTS leaves an existing table alone. To discard the pre-V2 demo
-- tables, run scripts/migrate_iceberg_v2.ps1 once, then submit this file.
--
-- Spark reads:
--   demo.aviation.aircraft_telemetry
--   demo.aviation.aircraft_telemetry_rejected
--   demo.aviation.aircraft_telemetry_1m
-- =============================================================


-- =============================================================
-- Submit
-- =============================================================

-- docker cp flink/flink_job.sql flink-jobmanager:/tmp/flink_job.sql
--
-- docker exec flink-jobmanager \
--   ./bin/sql-client.sh -f /tmp/flink_job.sql
--
-- or:
-- .\scripts\submit_flink_job.ps1
--
-- Flink UI:
-- http://localhost:8081
--
-- Query detail:
-- docker exec spark-iceberg spark-sql -e \
-- "SELECT * FROM demo.aviation.aircraft_telemetry LIMIT 20"
--
-- Query rejected:
-- docker exec spark-iceberg spark-sql -e \
-- "SELECT * FROM demo.aviation.aircraft_telemetry_rejected LIMIT 20"
--
-- Query 1-minute windows:
-- docker exec spark-iceberg spark-sql -e \
-- "SELECT * FROM demo.aviation.aircraft_telemetry_1m LIMIT 20"


-- =============================================================
-- 0. Job settings
-- =============================================================

SET 'pipeline.name' = 'lakehouse-aircraft-telemetry';

SET 'execution.runtime-mode' = 'streaming';

-- 3 matches the TaskManager slot count. The skew measurement used this
-- with 12 Kafka partitions. A savepoint taken at another parallelism
-- cannot be restored onto this setting.
SET 'parallelism.default' = '3';


-- Iceberg commits a snapshot on a successful checkpoint.
SET 'execution.checkpointing.interval' = '30s';

SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

-- Keep the last checkpoint on disk so a cancelled job can still be resumed.
-- Normal submit does not restore one. Resume only when a savepoint path is set.
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';

SET 'state.savepoints.dir' = 'file:///opt/flink/data/savepoints';

-- TaskManager loss should come back while the JobManager stays up.
-- 15 seconds gives the TaskManager container time to start again.
SET 'restart-strategy.type' = 'fixed-delay';

SET 'restart-strategy.fixed-delay.attempts' = '30';

SET 'restart-strategy.fixed-delay.delay' = '15 s';


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

    event_id            STRING,
    aircraft_id         STRING,
    flight_id           STRING,
    event_time          TIMESTAMP_LTZ(3),

    latitude            DOUBLE,
    longitude           DOUBLE,
    altitude_ft         DOUBLE,
    ground_speed_kts    DOUBLE,
    vertical_speed_fpm  DOUBLE,
    heading_deg         DOUBLE,
    engine_temp_c       DOUBLE,
    oil_pressure_psi    DOUBLE,
    fuel_flow_kg_h      DOUBLE,
    fuel_remaining_kg   DOUBLE,
    outside_air_temp_c  DOUBLE,

    kafka_partition INT
        METADATA FROM 'partition' VIRTUAL,
    kafka_offset BIGINT
        METADATA FROM 'offset' VIRTUAL,
    kafka_timestamp TIMESTAMP_LTZ(3)
        METADATA FROM 'timestamp' VIRTUAL,

    proc_time AS PROCTIME(),

    -- Five seconds of out-of-order data stay on time.
    WATERMARK FOR event_time
        AS event_time - INTERVAL '5' SECOND

)
WITH (

    'connector' = 'kafka',

    'topic' = 'aircraft-telemetry',

    'properties.bootstrap.servers' = 'kafka:19092',

    'properties.group.id' = 'lakehouse-flink-telemetry',

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
-- 2.1 Detail table
-- =============================================================

CREATE TABLE IF NOT EXISTS
lakehouse.aviation.aircraft_telemetry (

    event_id            STRING,
    aircraft_id         STRING,
    flight_id           STRING,
    event_time          TIMESTAMP_LTZ(6),
    ingest_time         TIMESTAMP_LTZ(6),

    latitude            DOUBLE,
    longitude           DOUBLE,
    altitude_ft         DOUBLE,
    ground_speed_kts    DOUBLE,
    vertical_speed_fpm  DOUBLE,
    heading_deg         DOUBLE,
    engine_temp_c       DOUBLE,
    oil_pressure_psi    DOUBLE,
    fuel_flow_kg_h      DOUBLE,
    fuel_remaining_kg   DOUBLE,
    outside_air_temp_c  DOUBLE,

    -- engine_temp_c > 1000. The row is still valid telemetry.
    overheat            BOOLEAN,

    -- Behind the watermark, but not more than 15 seconds behind.
    -- An open 1-minute window still counts these rows. A closed
    -- window does not.
    late                BOOLEAN

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
-- 2.2 Rejected table
-- =============================================================

CREATE TABLE IF NOT EXISTS
lakehouse.aviation.aircraft_telemetry_rejected (

    event_id            STRING,
    aircraft_id         STRING,
    flight_id           STRING,
    event_time          TIMESTAMP_LTZ(6),
    ingest_time         TIMESTAMP_LTZ(6),

    latitude            DOUBLE,
    longitude           DOUBLE,
    altitude_ft         DOUBLE,
    ground_speed_kts    DOUBLE,
    vertical_speed_fpm  DOUBLE,
    heading_deg         DOUBLE,
    engine_temp_c       DOUBLE,
    oil_pressure_psi    DOUBLE,
    fuel_flow_kg_h      DOUBLE,
    fuel_remaining_kg   DOUBLE,
    outside_air_temp_c  DOUBLE,

    reason              STRING,

    kafka_partition     INT,
    kafka_offset        BIGINT,
    kafka_timestamp     TIMESTAMP_LTZ(3)

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
-- 2.3 One-minute event-time aggregate
-- =============================================================
--
-- One row per aircraft_id, flight_id, and closed minute.
-- Emitted once, when the watermark reaches window_end.
-- Append-only: a closed minute is not updated.

CREATE TABLE IF NOT EXISTS
lakehouse.aviation.aircraft_telemetry_1m (

    aircraft_id              STRING,
    flight_id                STRING,
    window_start             TIMESTAMP_LTZ(3),
    window_end               TIMESTAMP_LTZ(3),

    event_count              BIGINT,
    avg_ground_speed_kts     DOUBLE,
    max_ground_speed_kts     DOUBLE,
    avg_altitude_ft          DOUBLE,
    max_altitude_ft          DOUBLE,
    avg_engine_temp_c        DOUBLE,
    max_engine_temp_c        DOUBLE,
    avg_fuel_flow_kg_h       DOUBLE,
    min_fuel_remaining_kg    DOUBLE,
    max_fuel_remaining_kg    DOUBLE,
    late_count               BIGINT

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

CREATE TEMPORARY VIEW telemetry_tagged AS

SELECT

    event_id,
    aircraft_id,
    flight_id,
    event_time,
    proc_time,

    latitude,
    longitude,
    altitude_ft,
    ground_speed_kts,
    vertical_speed_fpm,
    heading_deg,
    engine_temp_c,
    oil_pressure_psi,
    fuel_flow_kg_h,
    fuel_remaining_kg,
    outside_air_temp_c,

    CASE

        WHEN engine_temp_c IS NOT NULL
         AND engine_temp_c > 1000

        THEN TRUE

        ELSE FALSE

    END AS overheat,

    kafka_partition,
    kafka_offset,
    kafka_timestamp,

    COALESCE(

        event_time < CURRENT_WATERMARK(event_time),

        FALSE

    ) AS late,

    NULLIF(

        CONCAT_WS(',',

            CASE
                WHEN event_id IS NULL
                  OR TRIM(event_id) = ''
                THEN 'blank_event_id'
            END,

            CASE
                WHEN aircraft_id IS NULL
                  OR TRIM(aircraft_id) = ''
                THEN 'missing_aircraft_id'
            END,

            CASE
                WHEN flight_id IS NULL
                  OR TRIM(flight_id) = ''
                THEN 'missing_flight_id'
            END,

            CASE
                WHEN event_time IS NULL
                THEN 'missing_event_time'
            END,

            CASE
                WHEN engine_temp_c IS NULL
                THEN 'missing_engine_temp'
            END,

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
--
-- Overheat rows have no reject_reason, so they stay here.

CREATE TEMPORARY VIEW telemetry_clean AS

SELECT

    event_id,
    aircraft_id,
    flight_id,
    event_time,
    proc_time,

    latitude,
    longitude,
    altitude_ft,
    ground_speed_kts,
    vertical_speed_fpm,
    heading_deg,
    engine_temp_c,
    oil_pressure_psi,
    fuel_flow_kg_h,
    fuel_remaining_kg,
    outside_air_temp_c,

    overheat,
    late,

    kafka_partition,
    kafka_offset,
    kafka_timestamp

FROM telemetry_tagged

WHERE reject_reason IS NULL;



-- =============================================================
-- 5. First copy of each event_id
-- =============================================================
--
-- Processing-time order. State TTL = 1 hour.
-- The duplicate view below emits the later copies.

CREATE TEMPORARY VIEW telemetry_first AS

SELECT

    event_id,
    aircraft_id,
    flight_id,
    event_time,
    proc_time,

    latitude,
    longitude,
    altitude_ft,
    ground_speed_kts,
    vertical_speed_fpm,
    heading_deg,
    engine_temp_c,
    oil_pressure_psi,
    fuel_flow_kg_h,
    fuel_remaining_kg,
    outside_air_temp_c,

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
-- 6. Duplicate copies
-- =============================================================

CREATE TEMPORARY VIEW telemetry_duplicate AS

SELECT

    event_id,
    aircraft_id,
    flight_id,
    event_time,
    proc_time,

    latitude,
    longitude,
    altitude_ft,
    ground_speed_kts,
    vertical_speed_fpm,
    heading_deg,
    engine_temp_c,
    oil_pressure_psi,
    fuel_flow_kg_h,
    fuel_remaining_kg,
    outside_air_temp_c,

    kafka_partition,
    kafka_offset,
    kafka_timestamp

FROM telemetry_clean

MATCH_RECOGNIZE (

    PARTITION BY event_id
    ORDER BY proc_time

    MEASURES
        S.aircraft_id AS aircraft_id,
        S.flight_id AS flight_id,
        S.event_time AS event_time,
        S.proc_time AS proc_time,
        S.latitude AS latitude,
        S.longitude AS longitude,
        S.altitude_ft AS altitude_ft,
        S.ground_speed_kts AS ground_speed_kts,
        S.vertical_speed_fpm AS vertical_speed_fpm,
        S.heading_deg AS heading_deg,
        S.engine_temp_c AS engine_temp_c,
        S.oil_pressure_psi AS oil_pressure_psi,
        S.fuel_flow_kg_h AS fuel_flow_kg_h,
        S.fuel_remaining_kg AS fuel_remaining_kg,
        S.outside_air_temp_c AS outside_air_temp_c,
        S.kafka_partition AS kafka_partition,
        S.kafka_offset AS kafka_offset,
        S.kafka_timestamp AS kafka_timestamp

    ONE ROW PER MATCH

    AFTER MATCH SKIP TO LAST S

    PATTERN (A S)

    DEFINE

        A AS TRUE,

        S AS TRUE

);



-- =============================================================
-- 7. Write detail, rejected, and 1-minute aggregate
-- =============================================================

EXECUTE STATEMENT SET

BEGIN


INSERT INTO lakehouse.aviation.aircraft_telemetry

SELECT

    event_id,
    aircraft_id,
    flight_id,
    event_time,
    proc_time AS ingest_time,

    latitude,
    longitude,
    altitude_ft,
    ground_speed_kts,
    vertical_speed_fpm,
    heading_deg,
    engine_temp_c,
    oil_pressure_psi,
    fuel_flow_kg_h,
    fuel_remaining_kg,
    outside_air_temp_c,

    overheat,
    late

FROM telemetry_first;



INSERT INTO lakehouse.aviation.aircraft_telemetry_rejected

SELECT

    event_id,
    aircraft_id,
    flight_id,
    event_time,
    proc_time AS ingest_time,

    latitude,
    longitude,
    altitude_ft,
    ground_speed_kts,
    vertical_speed_fpm,
    heading_deg,
    engine_temp_c,
    oil_pressure_psi,
    fuel_flow_kg_h,
    fuel_remaining_kg,
    outside_air_temp_c,

    reason,
    kafka_partition,
    kafka_offset,
    kafka_timestamp

FROM (

    SELECT

        event_id,
        aircraft_id,
        flight_id,
        event_time,
        proc_time,

        latitude,
        longitude,
        altitude_ft,
        ground_speed_kts,
        vertical_speed_fpm,
        heading_deg,
        engine_temp_c,
        oil_pressure_psi,
        fuel_flow_kg_h,
        fuel_remaining_kg,
        outside_air_temp_c,

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
        flight_id,
        event_time,
        proc_time,

        latitude,
        longitude,
        altitude_ft,
        ground_speed_kts,
        vertical_speed_fpm,
        heading_deg,
        engine_temp_c,
        oil_pressure_psi,
        fuel_flow_kg_h,
        fuel_remaining_kg,
        outside_air_temp_c,

        'duplicate' AS reason,

        kafka_partition,
        kafka_offset,
        kafka_timestamp

    FROM telemetry_duplicate

);



-- Only rows still inside an unfired minute are aggregated.
-- late = TRUE rows are included while watermark < window_end.
-- After the watermark passes window_end the row is final.

INSERT INTO lakehouse.aviation.aircraft_telemetry_1m

SELECT

    aircraft_id,
    flight_id,
    window_start,
    window_end,

    COUNT(*) AS event_count,
    AVG(ground_speed_kts) AS avg_ground_speed_kts,
    MAX(ground_speed_kts) AS max_ground_speed_kts,
    AVG(altitude_ft) AS avg_altitude_ft,
    MAX(altitude_ft) AS max_altitude_ft,
    AVG(engine_temp_c) AS avg_engine_temp_c,
    MAX(engine_temp_c) AS max_engine_temp_c,
    AVG(fuel_flow_kg_h) AS avg_fuel_flow_kg_h,
    MIN(fuel_remaining_kg) AS min_fuel_remaining_kg,
    MAX(fuel_remaining_kg) AS max_fuel_remaining_kg,
    COUNT(CASE WHEN late THEN 1 END) AS late_count

FROM TABLE(

    TUMBLE(
        TABLE telemetry_first,
        DESCRIPTOR(event_time),
        INTERVAL '1' MINUTE
    )

)

GROUP BY

    aircraft_id,
    flight_id,
    window_start,
    window_end;


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
