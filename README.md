# Aircraft Telemetry Lakehouse

A streaming lakehouse for aircraft sensor events. A Python producer writes telemetry to Kafka. A Flink SQL job applies event-time watermarks, drops invalid and overheated readings, keeps the first copy of each `event_id`, and commits the result to Apache Iceberg. Rows that do not belong in the main table are stored separately, with a reason and the source Kafka offset.

The same watermark, dedup, and temperature rules are also implemented in pure Python, so the processing semantics can be demonstrated without a cluster.

```
producer  →  Kafka topic aircraft-telemetry  →  Flink SQL  →  Iceberg
  3 aircraft     late / duplicate / overheat       watermark · dedup · clean
                                                         │
                                         aircraft_telemetry
                                         aircraft_telemetry_rejected
```

## What this shows

- Event time with a 5-second watermark, and a 15-second lateness bound
- Exactly-once Iceberg commits on Flink checkpoints (RocksDB, 30-second interval)
- Append-only deduplication by `event_id`, with later copies routed to a reject table
- A reject table that records why a row was excluded and which Kafka offset it came from
- One Iceberg REST catalog read by Flink as `lakehouse` and by Spark as `demo`

## Pipeline

| Condition | Where it goes |
| --- | --- |
| Valid reading, first time this `event_id` is seen | `aviation.aircraft_telemetry` |
| Same `event_id` seen again within the 1-hour state TTL | `aviation.aircraft_telemetry_rejected`, `reason = duplicate` |
| `engine_temp > 1000` | rejected, `reason = overheat` |
| Event time is more than 15 seconds behind the watermark | rejected, `reason = too_late` |
| Event time is late but still inside 15 seconds | main table, `late = true` |
| Blank `event_id`, or missing aircraft, time, or temperature | rejected, `reason = blank_event_id` or `missing_*` |

Several problems on one row are joined with commas, for example `overheat,too_late`. Dedup state expires after one hour, so an `event_id` older than that is treated as new.

`event_time` is taken from the JSON field `timestamp`. Flink parses it as ISO-8601 with millisecond precision and a `Z` suffix, such as `2026-09-26T06:43:24.565Z`. Other shapes, including `+00:00` or six fractional digits, become null and are rejected as `missing_event_time`.

Iceberg makes a snapshot visible only after a successful checkpoint, so a new row can take about 30 seconds to show up in Spark.

## Layout

```
├── data_generator.py     # continuous producer for 3 aircraft
├── flink_job.py          # local simulation of watermark, dedup, and cleaning
├── flink_job.sql         # Flink SQL job: Kafka → Iceberg
├── send_test_event.py    # one Kafka message for a chosen scenario
├── submit_flink_job.ps1  # cancel the running job and submit flink_job.sql
├── requirements.txt
└── README.md
```

Generated locally and gitignored:

```
data/telemetry.jsonl
warehouse/iceberg/db/aircraft_telemetry/data.jsonl
```

## Event

```json
{
  "event_id": "7c2e0d2a-4f1b-4c0a-9a11-0b5e2d8c6f10",
  "aircraft_id": "AC-101",
  "timestamp": "2026-09-26T06:43:24.565Z",
  "telemetry": {
    "altitude": 10821.4,
    "speed": 854.2,
    "engine_temp": 688.1
  }
}
```

The producer injects faults so the job has something to reject:

| Fault | What is sent | Rate |
| --- | --- | --- |
| `late` | timestamp moved back 8–25 seconds | about half of the 5% anomaly budget |
| `duplicate` | previous `event_id` reused | the other half of that budget |
| `overheat` | `engine_temp` between 1050 and 1280 | 3% |

## Run the local simulation

No Kafka, Flink, or Spark required.

```powershell
python -m pip install -r requirements.txt
python data_generator.py --no-kafka
python flink_job.py --source file
```

`data_generator.py` prints one colored sensor line every 0.5 seconds and appends JSONL under `data/`. `flink_job.py` tails that file and prints:

```
[FLINK-WATERMARK] Reordered late event ...
[FLINK-DEDUP] Dropped duplicate event_id: ...
[FLINK-CLEAN] Dropped dirty record engine_temp=1124.0 > 1000
[FLINK-SINK] Written to Iceberg: db.aircraft_telemetry ...
```

The local job drops rejects instead of writing a second table. The reject table exists only in the Flink SQL job.

```powershell
python data_generator.py --interval 0.5 --anomaly-rate 0.05 --hot-rate 0.03
python flink_job.py --ooo 5 --lateness 15 --temp-limit 1000
```

If Kafka is reachable at `localhost:9092`, the producer also publishes to `aircraft-telemetry`. If it is not, the producer keeps writing JSONL.

## Run the Flink SQL job

`flink_job.sql` expects these services on one Docker network:

| Service | Address used by the job |
| --- | --- |
| Kafka | `kafka:19092`, topic `aircraft-telemetry` |
| Flink 1.19 SQL client | JobManager container `flink-jobmanager`, UI at `http://localhost:8081` |
| Iceberg REST catalog | `http://iceberg-rest:8181`, warehouse `s3://warehouse/` |
| MinIO | `http://minio:9000` |
| Spark SQL | container `spark-iceberg`, catalog name `demo` |

Flink needs the Kafka SQL connector and the Iceberg Flink runtime (plus the AWS bundle for S3FileIO) on the classpath. Spark's `demo` catalog and Flink's `lakehouse` catalog must both point at that REST catalog. The MinIO user and password in `flink_job.sql` are the local demo values `admin` / `password`.

Submit, replacing any running job of the same name. This cancels the old job and does not restore its savepoint, so dedup state starts empty. Kafka offsets still resume from the consumer group `lakehouse-flink-telemetry`.

```powershell
.\submit_flink_job.ps1
```

Send one message:

```powershell
python send_test_event.py normal
python send_test_event.py duplicate
python send_test_event.py overheat
python send_test_event.py watermark
python send_test_event.py too_late
```

`duplicate` reuses `TEST-NORMAL-001`, so send `normal` first. `too_late` is five minutes behind the clock; send `watermark` first so the watermark moves ahead of it. Wait for the next checkpoint before querying.

```powershell
docker exec spark-iceberg spark-sql -e "SELECT * FROM demo.aviation.aircraft_telemetry WHERE event_id = 'TEST-NORMAL-001'"
docker exec spark-iceberg spark-sql -e "SELECT event_id, reason, kafka_offset FROM demo.aviation.aircraft_telemetry_rejected LIMIT 20"
```

In Spark the tables are `demo.aviation.aircraft_telemetry` and `demo.aviation.aircraft_telemetry_rejected`. In Flink they are `lakehouse.aviation.*`. Both names are the same Iceberg tables.

## Stack

- Python 3, `pydantic`, `rich`, `kafka-python`
- Apache Kafka
- Apache Flink 1.19 (SQL, RocksDB, exactly-once checkpoints)
- Apache Iceberg (format v2, Parquet, zstd) on a REST catalog and MinIO
- Apache Spark for read queries
