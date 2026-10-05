# Real-Time Aircraft Telemetry Lakehouse

Aircraft telemetry arrives late, duplicated, and unevenly keyed. This project is a Kafka → Flink SQL → Iceberg → Spark pipeline that keeps valid readings, isolates data-quality failures, and recovers stream state after a task failure.

It is a reliability project: event time, bounded lateness, stateful deduplication, a 1-minute aggregate, checkpoint recovery, and a measured hot-key skew. It does not add an analytics layer, orchestration, or skew mitigation.

## Measured results

Flink 1.19.1, 5 October 2026. Full write-ups: [event time](docs/event-time.md), [failure recovery](docs/failure-recovery.md), [skew](docs/skew-experiment.md).

| Experiment | Result |
| --- | --- |
| Semantics (`tests/test_semantics.py`) | 15 passed, 0 failed |
| TaskManager kill, 100,000 unique ids | 100,000 produced, 100,000 Iceberg rows, 100,000 distinct ids, 0 missing, 0 unexpected, 0 duplicates |
| Savepoint stop and restore | Kafka progress, dedup state, and the open 1-minute window survived. A replayed `event_id` was rejected as `duplicate`. |
| Skew, 12 partitions, parallelism 3 | Hot aircraft 40%. Hottest Kafka partition 44.2%. Busiest Flink source subtask 59.4%. Backpressure stayed `ok`. |

The 100,000-id run shows no loss and no duplication for that TaskManager failure: the JobManager and checkpoint files stayed available, and the job restored from the latest completed checkpoint. Checkpoint mode is `EXACTLY_ONCE`. That setting lines the Kafka offset up with the Iceberg commit. It is not a universal exactly-once guarantee.

A checkpoint is automatic recovery. A savepoint is a planned stop and resume. `scripts/submit_flink_job.ps1` does neither.

The skew run measured imbalance. It did not measure backpressure, and no mitigation was added.

## Architecture

![Kafka to Flink to Iceberg to Spark](docs/images/architecture.png)

```text
Python producers
    → Kafka topic aircraft-telemetry, key = aircraft_id
    → Flink SQL
        → aircraft_telemetry            first valid event_id
        → aircraft_telemetry_rejected   data-quality and duplicate rows
        → aircraft_telemetry_1m         closed event-time minute
    → Iceberg REST + Parquet on MinIO
    → Spark catalog demo
```

Flink uses catalog `lakehouse`. Spark uses catalog `demo`. Both point at the same REST catalog and warehouse.

| Table | What it stores |
| --- | --- |
| `aircraft_telemetry` | First valid copy of each `event_id`. `overheat` and `late` are flags, not reject reasons. |
| `aircraft_telemetry_rejected` | Blank or missing fields, `too_late`, and later copies of an `event_id`. Keeps `reason`, Kafka partition, offset, and timestamp. |
| `aircraft_telemetry_1m` | One row per `aircraft_id`, `flight_id`, and closed event-time minute. Append-only after the watermark passes `window_end`. |

![Flink processing flow](docs/images/flink-processing-flow.png)

Invalid rows are rejected before dedup. `engine_temp_c > 1000` stays on the detail table with `overheat = true`. Several missing fields can share one reason string, for example `blank_event_id,missing_engine_temp`.

## Engineering challenges

| Challenge | What the job does |
| --- | --- |
| Event time and watermark | `event_time - 5 seconds`. Idle Kafka partitions stop blocking the watermark after 10 seconds. |
| Late events | Within 15 seconds behind the watermark: detail row, `late = true`, counted if that minute is still open. Older than that: `too_late`, rejected, absent from the minute. |
| Stateful dedup | First `event_id` by processing time goes to the detail table and the window. Later copies within a 1-hour state TTL are `duplicate`. |
| 1-minute window | `TUMBLE` of 1 minute on event time, grouped by `aircraft_id` and `flight_id`. A closed minute is not revised. |
| Checkpoint recovery | 30-second RocksDB checkpoints. A killed TaskManager restarted on the same job id from the last successful checkpoint. |
| Savepoint recovery | `flink stop` with a savepoint, then the same SQL graph restored from that path. Parallelism was unchanged. |
| Kafka / Flink skew | Keyed by `aircraft_id`. One aircraft on one partition, and that partition shared a source subtask with three others. |

Details and the window case table are in [docs/event-time.md](docs/event-time.md).

## Data model

V2 is one flat reading per event. `aircraft_id` is the Kafka key. `aircraft_id` and `flight_id` are the join keys left for a later analytics project. This repository does not build flight, aircraft, airport, or weather tables.

```json
{
  "event_id": "7c2e0d2a-4f1b-4c0a-9a11-0b5e2d8c6f10",
  "aircraft_id": "AC-101",
  "flight_id": "AC-101-20261005-01",
  "event_time": "2026-10-05T08:15:00.000Z",
  "latitude": 32.947,
  "longitude": 120.752,
  "altitude_ft": 30276,
  "ground_speed_kts": 393,
  "vertical_speed_fpm": 2577,
  "heading_deg": 342.1,
  "engine_temp_c": 790,
  "oil_pressure_psi": 55.1,
  "fuel_flow_kg_h": 3100,
  "fuel_remaining_kg": 16142,
  "outside_air_temp_c": -44.9
}
```

The generator moves position, altitude, speed, and fuel together by flight phase. The 1,000°C threshold is a demo quality rule, not an aircraft limit. Replacing the old demo tables is an explicit one-time reset: `scripts/migrate_iceberg_v2.ps1`. Normal submit only creates missing tables.

## Design decisions and limitations

- Kafka key is `aircraft_id`, so one aircraft stays on one partition. `event_id` is the business identity used for dedup. Those are different keys, and the skew test shows the cost.
- Event time is separate from arrival time. The 5-second watermark allows out-of-order data. The extra 15 seconds is the business late policy, not the watermark itself.
- Dedup state expires after one hour. An old `event_id` can then be accepted again.
- RocksDB holds operator state. A checkpoint snapshots that state and the Kafka offsets. Dedup does not replace a checkpoint, and a checkpoint does not decide which business event is a duplicate.
- Iceberg, not a directory of Parquet files, so Flink and Spark share snapshots. Streaming commits make a row visible on the next successful checkpoint, often about 30 seconds later, longer while a backlog drains.
- Rejected rows stay queryable, with the Kafka position that produced them.
- Checkpoints live on a shared local volume (`file:///opt/flink/data/...`), not remote durable storage. The failure test did not kill the JobManager or delete checkpoint files.
- The running job is parallelism 3 on 12 partitions, from a fresh submit. The savepoint and the 100,000-id kill were parallelism 1. A savepoint was not restored onto a different parallelism.
- No salting, second aggregation key, or other skew mitigation.

## Repository

```text
src/telemetry_event.py          V2 message and flight track
src/producers/                  generator, scripted sender, bulk sender
src/simulator/flink_job.py      local stand-in, not the cluster job
flink/flink_job.sql             cluster job
scripts/                        submit, savepoint, one-time Iceberg reset
tests/                          semantics, savepoint, 100k kill, skew
docs/                           event time, recovery, skew, diagrams
```

## Run

Services expected on one Docker network: Kafka `kafka:19092`, Flink 1.19 JobManager `localhost:8081`, Iceberg REST `iceberg-rest:8181`, MinIO `minio:9000`, Spark SQL container `spark-iceberg`.

```powershell
python -m pip install -r requirements.txt
.\scripts\submit_flink_job.ps1
```

Submit refuses a second `lakehouse-aircraft-telemetry` job. It does not cancel a job, drop tables, or restore a savepoint. Cancel the running job before submitting again.

```powershell
python tests/test_semantics.py
python tests/test_savepoint.py
python tests/test_recovery_100k.py
python tests/test_skew.py
```

`test_savepoint.py` stops the running job. `test_recovery_100k.py` kills and restarts `flink-taskmanager`. `test_skew.py` expects 12 partitions and parallelism 3.

Planned restart, same parallelism:

```powershell
.\scripts\savepoint.ps1 stop
.\scripts\savepoint.ps1 resume
```

Local simulation, no Kafka or Flink:

```powershell
python src/producers/data_generator.py --no-kafka
python src/simulator/flink_job.py --source file
```

Scripted Kafka messages: `python src/producers/send_test_event.py normal` and the same script with `duplicate`, `overheat`, `watermark`, or `too_late`.

Spark checks:

```powershell
docker exec spark-iceberg spark-sql -e "SELECT * FROM demo.aviation.aircraft_telemetry ORDER BY ingest_time DESC LIMIT 20"
docker exec spark-iceberg spark-sql -e "SELECT event_id, reason, kafka_partition, kafka_offset FROM demo.aviation.aircraft_telemetry_rejected ORDER BY ingest_time DESC LIMIT 20"
docker exec spark-iceberg spark-sql -e "SELECT * FROM demo.aviation.aircraft_telemetry_1m ORDER BY window_start DESC LIMIT 20"
```
