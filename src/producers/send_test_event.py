"""Send aircraft-telemetry events to Kafka.

Pick a scenario:

    python src/producers/send_test_event.py normal
    python src/producers/send_test_event.py duplicate
    python src/producers/send_test_event.py overheat
    python src/producers/send_test_event.py watermark
    python src/producers/send_test_event.py too_late
    python src/producers/send_test_event.py batch

Timestamps use millisecond precision and a Z suffix so Flink's ISO-8601
parser accepts them. Send `normal` before `duplicate`. Send `watermark`
before `too_late`.

`batch` publishes 100 V2 messages for one aircraft, then prints the Iceberg
checks for that run. The Flink job must already be running. Iceberg commits
on checkpoint, so wait about 30 seconds after the script exits before querying.

Deterministic watermark / late / window checks live in tests/test_semantics.py.
"""

import argparse
import json
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from kafka import KafkaProducer

_SRC = Path(__file__).resolve().parents[1]
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from telemetry_event import iso_z, message

TOPIC = "aircraft-telemetry"
# Same key on every batch record so Kafka keeps send order in one partition.
BATCH_ROUTING_KEY = b"AC-101"


def build_messages(now: datetime) -> dict:
    stamp = now
    return {
        "normal": message(
            "TEST-NORMAL-001", "AC-101", "AC-101-TEST-01", stamp
        ),
        "duplicate": message(
            "TEST-NORMAL-001", "AC-101", "AC-101-TEST-01", stamp
        ),
        # engine_temp_c > 1000. Stays in the detail table with overheat = TRUE.
        "overheat": message(
            "TEST-OVERHEAT-001",
            "AC-202",
            "AC-202-TEST-01",
            stamp,
            altitude_ft=12000,
            ground_speed_kts=250,
            vertical_speed_fpm=1500,
            engine_temp_c=1100,
            fuel_flow_kg_h=3200,
        ),
        "watermark": message(
            "TEST-WATERMARK-007",
            "AC-404",
            "AC-404-TEST-01",
            now - timedelta(seconds=240),
        ),
        "too_late": message(
            "TEST-TOO-LATE-002",
            "AC-404",
            "AC-404-TEST-01",
            now - timedelta(minutes=5),
            altitude_ft=10500,
            ground_speed_kts=220,
            engine_temp_c=640,
        ),
    }


def build_batch(now: datetime) -> tuple[list[dict], list[dict], dict]:
    """Build 100 V2 messages.

    Phase 1 uses one timestamp 30s ahead of now. That moves the watermark to
    anchor-5s. Phase 2 is sent after a pause:

        late      = anchor-10s  → detail.late = TRUE, and the 1-minute
                                  window counts it if that minute is still open
        too_late  = anchor-60s  → rejected, reason = too_late
    """
    token = uuid.uuid4().hex[:6]
    aircraft_id = f"ACB{token}"
    flight_id = f"{aircraft_id}-LEG01"
    prefix = f"BATCH-{token}"
    anchor = now + timedelta(seconds=30)
    on_time = anchor
    late_at = anchor - timedelta(seconds=10)
    too_late_at = anchor - timedelta(seconds=60)

    def ident(kind: str, index: int) -> str:
        return f"{prefix}-{kind}-{index:03d}"

    def row(event_id, when, **overrides):
        return message(event_id, aircraft_id, flight_id, when, **overrides)

    phase1: list[dict] = []
    for index in range(37):
        phase1.append(row(ident("N", index), on_time))
    for index in range(8):
        phase1.append(row(ident("N", index), on_time))
    for index in range(15):
        phase1.append(
            row(
                ident("H", index),
                on_time,
                altitude_ft=12000,
                ground_speed_kts=250,
                vertical_speed_fpm=1200,
                engine_temp_c=1100,
                fuel_flow_kg_h=3300,
            )
        )
    for index, event_id in enumerate(("", "", "   ")):
        phase1.append(row(event_id, on_time, altitude_ft=10000 + index))
    phase1.append(row("", on_time, altitude_ft=10099, engine_temp_c=None))
    for index in range(4):
        phase1.append(
            message(ident("NOAC", index), None, flight_id, on_time, altitude_ft=10100)
        )
    for index in range(3):
        phase1.append(
            message(ident("NOFL", index), aircraft_id, None, on_time, altitude_ft=10150)
        )
    for index in range(3):
        phase1.append(message(ident("NOTS", index), aircraft_id, flight_id, None))
    for index in range(3):
        phase1.append(
            row(ident("NOTEMP", index), on_time, altitude_ft=10300, engine_temp_c=None)
        )

    phase2: list[dict] = []
    for index in range(10):
        phase2.append(
            row(
                ident("L", index),
                late_at,
                altitude_ft=28000,
                ground_speed_kts=400,
                vertical_speed_fpm=-800,
                engine_temp_c=600,
            )
        )
    for index in range(5):
        phase2.append(
            row(
                ident("LH", index),
                late_at,
                altitude_ft=18000,
                ground_speed_kts=320,
                vertical_speed_fpm=1600,
                engine_temp_c=1150,
                fuel_flow_kg_h=3400,
            )
        )
    for index in range(8):
        phase2.append(
            row(
                ident("TL", index),
                too_late_at,
                altitude_ft=8000,
                ground_speed_kts=210,
                engine_temp_c=540,
            )
        )

    # 37 normals + 8 duplicate copies + 15 overheat + 20 data-quality
    # + 10 late + 5 late-overheat + 8 too-late = 100.
    # Three normals were given to missing_flight_id so the batch stays at 100.
    if len(phase1) + len(phase2) != 100:
        raise RuntimeError(
            f"batch must contain 100 messages, got {len(phase1) + len(phase2)}"
        )

    plan = {
        "token": token,
        "aircraft_id": aircraft_id,
        "flight_id": flight_id,
        "prefix": prefix,
        "on_time": iso_z(on_time),
        "late_at": iso_z(late_at),
        "too_late_at": iso_z(too_late_at),
        "main_rows": 67,
        "rejected_rows": 33,
        "main": {
            "normal": "37  overheat=false  late=false",
            "overheat": "15  overheat=true   late=false",
            "late": "10  overheat=false  late=true",
            "late_overheat": "5   overheat=true   late=true",
        },
        "rejected": {
            "duplicate": 8,
            "too_late": 8,
            "blank_event_id": 3,
            "blank_event_id,missing_engine_temp": 1,
            "missing_aircraft_id": 4,
            "missing_flight_id": 3,
            "missing_event_time": 3,
            "missing_engine_temp": 3,
        },
    }
    return phase1, phase2, plan


def _produce(producer: KafkaProducer, messages: list[dict]) -> list[int]:
    futures = [
        producer.send(TOPIC, key=BATCH_ROUTING_KEY, value=message)
        for message in messages
    ]
    producer.flush()
    return [future.get(timeout=10).partition for future in futures]


def send_batch(bootstrap: str, settle_seconds: float) -> None:
    """Send one 100-message mix and print the Iceberg checks for this run."""
    phase1, phase2, plan = build_batch(datetime.now(timezone.utc))
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )
    try:
        first = _produce(producer, phase1)
        print(f"phase 1: {len(phase1)} messages on partition {sorted(set(first))}")
        print(
            f"waiting {settle_seconds:g}s so the watermark can pass "
            f"the late and too_late timestamps..."
        )
        time.sleep(settle_seconds)
        second = _produce(producer, phase2)
        print(f"phase 2: {len(phase2)} messages on partition {sorted(set(second))}")
    finally:
        producer.close()

    _print_batch_report(plan)


def _print_batch_report(plan: dict) -> None:
    aircraft_id = plan["aircraft_id"]
    flight_id = plan["flight_id"]
    token = plan["token"]
    print(f"\nbatch {token}  aircraft_id={aircraft_id}  flight_id={flight_id}  messages=100")
    print(f"on_time={plan['on_time']}")
    print(f"late   ={plan['late_at']}   (detail.late = true; counted if that minute is still open)")
    print(f"too_late={plan['too_late_at']}   (rejected.reason = too_late)")
    print(f"\nExpect in aviation.aircraft_telemetry ({plan['main_rows']} rows):")
    for kind, detail in plan["main"].items():
        print(f"  {kind:<16} {detail}")
    print(f"\nExpect in aviation.aircraft_telemetry_rejected ({plan['rejected_rows']} rows):")
    for reason, count in plan["rejected"].items():
        print(f"  {count:>2}  {reason}")
    print(
        f"""
Wait for the next Flink checkpoint (about 30s), then run:

SELECT kind,
       COUNT(*) AS cnt,
       SUM(CASE WHEN overheat THEN 1 ELSE 0 END) AS overheat_true,
       SUM(CASE WHEN late THEN 1 ELSE 0 END) AS late_true
FROM (
    SELECT
        CASE
            WHEN event_id LIKE 'BATCH-{token}-LH-%' THEN 'late_overheat'
            WHEN event_id LIKE 'BATCH-{token}-L-%' THEN 'late'
            WHEN event_id LIKE 'BATCH-{token}-H-%' THEN 'overheat'
            WHEN event_id LIKE 'BATCH-{token}-N-%' THEN 'normal'
            ELSE 'other'
        END AS kind,
        overheat,
        late
    FROM aviation.aircraft_telemetry
    WHERE flight_id = '{flight_id}'
) t
GROUP BY kind
ORDER BY kind;

SELECT reason, COUNT(*) AS cnt
FROM aviation.aircraft_telemetry_rejected
WHERE flight_id = '{flight_id}'
   OR event_id LIKE 'BATCH-{token}-NOAC-%'
   OR event_id LIKE 'BATCH-{token}-NOFL-%'
GROUP BY reason
ORDER BY reason;
"""
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Send aircraft telemetry test events")
    parser.add_argument(
        "scenario",
        choices=("normal", "duplicate", "overheat", "watermark", "too_late", "batch"),
    )
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument(
        "--settle",
        type=float,
        default=5.0,
        help="Seconds to wait inside `batch` before sending late events",
    )
    args = parser.parse_args()

    if args.scenario == "batch":
        send_batch(args.bootstrap, args.settle)
        return

    payload = build_messages(datetime.now(timezone.utc))[args.scenario]
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )
    try:
        result = producer.send(
            TOPIC, key=payload["aircraft_id"].encode("utf-8"), value=payload
        ).get(timeout=10)
    finally:
        producer.close()

    print("\nSent:")
    print(json.dumps(payload, indent=2))
    print(f"\npartition={result.partition}, offset={result.offset}")


if __name__ == "__main__":
    main()
