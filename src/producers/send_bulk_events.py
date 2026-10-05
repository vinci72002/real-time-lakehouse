"""Send 100,000 aircraft-telemetry messages to Kafka.

    python src/producers/send_bulk_events.py
    python src/producers/send_bulk_events.py --count 100000 --bootstrap localhost:9092

The Flink job must already be running. Rows show up in Iceberg after Flink
has consumed them and completed a checkpoint. Parallelism is 1, so consuming
100,000 records takes longer than sending them.

Late and too_late are not in this mix. Those flags depend on the watermark
moving between records; use `src/producers/send_test_event.py batch` for that check.

Every record is stamped two minutes ahead of now, so a watermark left by the
small batch (anchor = now + 30s) does not mark this run too_late.
"""

import argparse
import json
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from kafka import KafkaProducer
from kafka.partitioner.default import DefaultPartitioner

_SRC = Path(__file__).resolve().parents[1]
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from telemetry_event import iso_z, message

TOPIC = "aircraft-telemetry"
# murmur2 % 3 for a 3-partition topic: AC-101 -> 0, AC-103 -> 1, AC-201 -> 2.
ROUTING_KEYS = (b"AC-101", b"AC-103", b"AC-201")
EVENT_LEAD = timedelta(minutes=2)

def _bulk_row(
    event_id: str | None,
    aircraft_id: str | None,
    flight_id: str | None,
    stamp: datetime | None,
    *,
    altitude_ft: float,
    ground_speed_kts: float,
    engine_temp_c: float | None,
) -> dict:
    return message(
        event_id,
        aircraft_id,
        flight_id,
        stamp,
        altitude_ft=altitude_ft,
        ground_speed_kts=ground_speed_kts,
        engine_temp_c=engine_temp_c,
        vertical_speed_fpm=0,
        fuel_flow_kg_h=2400 if (engine_temp_c or 0) <= 1000 else 3300,
    )


def mix_for(count: int) -> dict[str, int]:
    """Split `count` into the sizes Iceberg should show for this run.

    70% normal, 15% overheat, 5% duplicate, and the rest split across the
    four data-quality rejects. Duplicate copies reuse normal event ids, so
    there must be at least as many normals as duplicates.
    """
    if count < 20:
        raise SystemExit("count must be at least 20")

    normal = count * 70 // 100
    overheat = count * 15 // 100
    duplicate = count * 5 // 100
    rest = count - normal - overheat - duplicate
    base, extra = divmod(rest, 4)
    quality = [
        "blank_event_id",
        "missing_aircraft_id",
        "missing_event_time",
        "missing_engine_temp",
    ]
    mix = {
        "normal": normal,
        "overheat": overheat,
        "duplicate": duplicate,
    }
    for index, name in enumerate(quality):
        mix[name] = base + (1 if index < extra else 0)

    if duplicate > normal:
        raise SystemExit("duplicate count cannot exceed normal count")
    if sum(mix.values()) != count:
        raise RuntimeError(f"mix sums to {sum(mix.values())}, expected {count}")
    return mix


def iter_primary(token: str, stamp: datetime, mix: dict[str, int]):
    """First wave. Originals are acknowledged before any duplicate is sent."""
    aircraft_id = f"ACB{token}"
    flight_id = f"{aircraft_id}-LEG01"
    prefix = f"BULK-{token}"

    for index in range(mix["normal"]):
        yield ROUTING_KEYS[index % 3], _bulk_row(
            f"{prefix}-N-{index:06d}",
            aircraft_id,
            flight_id,
            stamp,
            altitude_ft=35000,
            ground_speed_kts=455,
            engine_temp_c=680,
        )
    for index in range(mix["overheat"]):
        yield ROUTING_KEYS[index % 3], _bulk_row(
            f"{prefix}-H-{index:06d}",
            aircraft_id,
            flight_id,
            stamp,
            altitude_ft=18000,
            ground_speed_kts=320,
            engine_temp_c=1100,
        )
    for index in range(mix["blank_event_id"]):
        yield ROUTING_KEYS[index % 3], _bulk_row(
            "",
            aircraft_id,
            flight_id,
            stamp,
            altitude_ft=10000,
            ground_speed_kts=220,
            engine_temp_c=640,
        )
    for index in range(mix["missing_aircraft_id"]):
        yield ROUTING_KEYS[index % 3], _bulk_row(
            f"{prefix}-NOAC-{index:06d}",
            None,
            flight_id,
            stamp,
            altitude_ft=10100,
            ground_speed_kts=210,
            engine_temp_c=630,
        )
    for index in range(mix["missing_event_time"]):
        yield ROUTING_KEYS[index % 3], _bulk_row(
            f"{prefix}-NOTS-{index:06d}",
            aircraft_id,
            flight_id,
            None,
            altitude_ft=10200,
            ground_speed_kts=200,
            engine_temp_c=620,
        )
    for index in range(mix["missing_engine_temp"]):
        yield ROUTING_KEYS[index % 3], _bulk_row(
            f"{prefix}-NOTEMP-{index:06d}",
            aircraft_id,
            flight_id,
            stamp,
            altitude_ft=10300,
            ground_speed_kts=190,
            engine_temp_c=None,
        )


def iter_duplicates(token: str, stamp: datetime, mix: dict[str, int]):
    """Second copies of the first normal event ids. Same key as the original."""
    aircraft_id = f"ACB{token}"
    flight_id = f"{aircraft_id}-LEG01"
    prefix = f"BULK-{token}"
    for index in range(mix["duplicate"]):
        yield ROUTING_KEYS[index % 3], _bulk_row(
            f"{prefix}-N-{index:06d}",
            aircraft_id,
            flight_id,
            stamp,
            altitude_ft=35000,
            ground_speed_kts=455,
            engine_temp_c=680,
        )


def _produce(producer: KafkaProducer, records, total: int, sent: int) -> int:
    pending = []
    for key, value in records:
        pending.append(producer.send(TOPIC, key=key, value=value))
        sent += 1
        if sent % 10_000 == 0 or sent == total:
            producer.flush()
            for future in pending:
                future.get(timeout=30)
            pending.clear()
            print(f"sent {sent}/{total}", flush=True)
    if pending:
        producer.flush()
        for future in pending:
            future.get(timeout=30)
    return sent


def send_bulk(bootstrap: str, count: int) -> None:
    token = uuid.uuid4().hex[:6]
    mix = mix_for(count)
    stamp = datetime.now(timezone.utc) + EVENT_LEAD
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        linger_ms=20,
        batch_size=64 * 1024,
        retries=3,
    )
    started = time.perf_counter()
    try:
        partitions = producer.partitions_for(TOPIC)
        if not partitions:
            raise SystemExit(f"topic {TOPIC} has no partitions")
        _print_routing(partitions)
        sent = _produce(producer, iter_primary(token, stamp, mix), count, 0)
        sent = _produce(
            producer, iter_duplicates(token, stamp, mix), count, sent
        )
        if sent != count:
            raise RuntimeError(f"sent {sent}, expected {count}")
    finally:
        producer.close()

    elapsed = time.perf_counter() - started
    _print_report(token, stamp, mix, elapsed)


def _print_routing(partitions: set[int]) -> None:
    ordered = sorted(partitions)
    print(f"topic partitions: {ordered}")
    if len(ordered) != 3:
        print(
            "routing keys AC-101, AC-103, AC-201 were chosen for 3 partitions; "
            f"this topic has {len(ordered)}"
        )
        return
    partitioner = DefaultPartitioner()
    for key in ROUTING_KEYS:
        print(f"  key {key.decode()} -> partition {partitioner(key, ordered, ordered)}")


def _print_report(token: str, stamp: str, mix: dict[str, int], elapsed: float) -> None:
    main_rows = mix["normal"] + mix["overheat"]
    rejected_rows = (
        mix["duplicate"]
        + mix["blank_event_id"]
        + mix["missing_aircraft_id"]
        + mix["missing_event_time"]
        + mix["missing_engine_temp"]
    )
    print(
        f"\nbatch {token}  aircraft_id=ACB{token}  "
        f"messages={main_rows + rejected_rows}  elapsed={elapsed:.1f}s"
    )
    print(f"event_time={iso_z(stamp)}")
    print(f"\nExpect in aviation.aircraft_telemetry ({main_rows} rows):")
    print(f"  normal     {mix['normal']:<6} overheat=false  late=false")
    print(f"  overheat   {mix['overheat']:<6} overheat=true   late=false")
    print(f"\nExpect in aviation.aircraft_telemetry_rejected ({rejected_rows} rows):")
    print(f"  {mix['duplicate']:<6} duplicate")
    print(f"  {mix['blank_event_id']:<6} blank_event_id")
    print(f"  {mix['missing_aircraft_id']:<6} missing_aircraft_id")
    print(f"  {mix['missing_event_time']:<6} missing_event_time")
    print(f"  {mix['missing_engine_temp']:<6} missing_engine_temp")
    print(
        f"""
Wait until Flink has consumed this run and checkpointed, then run:

SELECT kind,
       COUNT(*) AS cnt,
       SUM(CASE WHEN overheat THEN 1 ELSE 0 END) AS overheat_true,
       SUM(CASE WHEN late THEN 1 ELSE 0 END) AS late_true
FROM (
    SELECT
        CASE
            WHEN event_id LIKE 'BULK-{token}-H-%' THEN 'overheat'
            WHEN event_id LIKE 'BULK-{token}-N-%' THEN 'normal'
            ELSE 'other'
        END AS kind,
        overheat,
        late
    FROM aviation.aircraft_telemetry
    WHERE event_id LIKE 'BULK-{token}-%'
) t
GROUP BY kind
ORDER BY kind;

SELECT reason, COUNT(*) AS cnt
FROM aviation.aircraft_telemetry_rejected
WHERE event_id LIKE 'BULK-{token}-%'
   OR (
        aircraft_id = 'ACB{token}'
        AND (event_id IS NULL OR TRIM(event_id) = '')
      )
GROUP BY reason
ORDER BY reason;
"""
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Send a bulk aircraft-telemetry mix to Kafka"
    )
    parser.add_argument("--count", type=int, default=100_000)
    parser.add_argument("--bootstrap", default="localhost:9092")
    args = parser.parse_args()
    send_bulk(args.bootstrap, args.count)


if __name__ == "__main__":
    main()
