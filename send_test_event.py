"""Send aircraft-telemetry events to Kafka.

Pick a scenario:

    python send_test_event.py normal
    python send_test_event.py duplicate
    python send_test_event.py overheat
    python send_test_event.py watermark
    python send_test_event.py too_late
    python send_test_event.py batch

Timestamps use millisecond precision and a Z suffix so Flink's ISO-8601
parser accepts them. Send `normal` before `duplicate`. Send `watermark`
before `too_late`.

`batch` publishes 100 messages for one aircraft, then prints the Iceberg
checks for that run. The Flink job must already be running. Iceberg commits
on checkpoint, so wait about 30 seconds after the script exits before querying.
"""

import argparse
import json
import time
import uuid
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer

TOPIC = "aircraft-telemetry"
# Same key on every batch record so Kafka keeps send order in one partition.
# Late / too_late flags are decided from that partition's watermark.
BATCH_ROUTING_KEY = b"AC-101"


def iso_z(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def build_messages(now: datetime) -> dict:
    stamp = iso_z(now)
    return {
        # Accepted into aircraft_telemetry.
        "normal": {
            "event_id": "TEST-NORMAL-001",
            "aircraft_id": "AC-101",
            "timestamp": stamp,
            "telemetry": {"altitude": 10800, "speed": 860, "engine_temp": 690},
        },
        # Same event_id as normal. Lands in the reject table as duplicate.
        "duplicate": {
            "event_id": "TEST-NORMAL-001",
            "aircraft_id": "AC-101",
            "timestamp": stamp,
            "telemetry": {"altitude": 10800, "speed": 860, "engine_temp": 690},
        },
        # engine_temp > 1000. Stays in the main table with overheat = TRUE.
        "overheat": {
            "event_id": "TEST-OVERHEAT-001",
            "aircraft_id": "AC-202",
            "timestamp": stamp,
            "telemetry": {"altitude": 12000, "speed": 820, "engine_temp": 1100},
        },
        # Moves the watermark forward so a following too_late event can be judged.
        "watermark": {
            "event_id": "TEST-WATERMARK-007",
            "aircraft_id": "AC-404",
            "timestamp": iso_z(now - timedelta(seconds=240)),
            "telemetry": {"altitude": 11000, "speed": 850, "engine_temp": 700},
        },
        # Five minutes behind the watermark anchor. Rejected as too_late.
        "too_late": {
            "event_id": "TEST-TOO-LATE-002",
            "aircraft_id": "AC-404",
            "timestamp": iso_z(now - timedelta(minutes=5)),
            "telemetry": {"altitude": 10500, "speed": 800, "engine_temp": 680},
        },
    }


def _event(
    event_id: str | None,
    aircraft_id: str | None,
    timestamp: str | None,
    altitude: float,
    speed: float,
    engine_temp: float | None,
) -> dict:
    return {
        "event_id": event_id,
        "aircraft_id": aircraft_id,
        "timestamp": timestamp,
        "telemetry": {
            "altitude": altitude,
            "speed": speed,
            "engine_temp": engine_temp,
        },
    }


def build_batch(now: datetime) -> tuple[list[dict], list[dict], dict]:
    """Build 100 messages whose Iceberg landing is fixed by flink_job.sql.

    Phase 1 uses one timestamp 30s ahead of now. That moves the watermark to
    anchor-5s. Phase 2 is sent after a pause:

        late      = anchor-10s  → main table, late = TRUE
        too_late  = anchor-60s  → rejected, reason = too_late
    """
    token = uuid.uuid4().hex[:6]
    aircraft_id = f"AC-B{token}"
    prefix = f"BATCH-{token}"
    anchor = now + timedelta(seconds=30)
    on_time = iso_z(anchor)
    late_at = iso_z(anchor - timedelta(seconds=10))
    too_late_at = iso_z(anchor - timedelta(seconds=60))

    def ident(kind: str, index: int) -> str:
        return f"{prefix}-{kind}-{index:03d}"

    phase1: list[dict] = []
    for index in range(40):
        phase1.append(_event(ident("N", index), aircraft_id, on_time, 10800, 860, 690))
    for index in range(8):
        phase1.append(_event(ident("N", index), aircraft_id, on_time, 10800, 860, 690))
    for index in range(15):
        phase1.append(_event(ident("H", index), aircraft_id, on_time, 12000, 820, 1100))
    for index, event_id in enumerate(("", "", "   ")):
        phase1.append(_event(event_id, aircraft_id, on_time, 10000 + index, 800, 640))
    phase1.append(_event("", aircraft_id, on_time, 10099, 800, None))
    for index in range(4):
        phase1.append(_event(ident("NOAC", index), None, on_time, 10100, 790, 630))
    for index in range(3):
        phase1.append(_event(ident("NOTS", index), aircraft_id, None, 10200, 780, 620))
    for index in range(3):
        phase1.append(_event(ident("NOTEMP", index), aircraft_id, on_time, 10300, 770, None))

    phase2: list[dict] = []
    for index in range(10):
        phase2.append(_event(ident("L", index), aircraft_id, late_at, 10500, 800, 680))
    for index in range(5):
        phase2.append(_event(ident("LH", index), aircraft_id, late_at, 11900, 810, 1150))
    for index in range(8):
        phase2.append(_event(ident("TL", index), aircraft_id, too_late_at, 10000, 780, 650))

    if len(phase1) + len(phase2) != 100:
        raise RuntimeError(f"batch must contain 100 messages, got {len(phase1) + len(phase2)}")

    plan = {
        "token": token,
        "aircraft_id": aircraft_id,
        "prefix": prefix,
        "on_time": on_time,
        "late_at": late_at,
        "too_late_at": too_late_at,
        "main": {
            "normal": "40  overheat=false  late=false",
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
        print(
            f"phase 1: {len(phase1)} messages on partition {sorted(set(first))}"
        )
        print(
            f"waiting {settle_seconds:g}s so the watermark can pass "
            f"the late and too_late timestamps..."
        )
        time.sleep(settle_seconds)
        second = _produce(producer, phase2)
        print(
            f"phase 2: {len(phase2)} messages on partition {sorted(set(second))}"
        )
    finally:
        producer.close()

    _print_batch_report(plan)


def _print_batch_report(plan: dict) -> None:
    aircraft_id = plan["aircraft_id"]
    token = plan["token"]
    print(f"\nbatch {token}  aircraft_id={aircraft_id}  messages=100")
    print(f"on_time={plan['on_time']}")
    print(f"late   ={plan['late_at']}   (main.late = true)")
    print(f"too_late={plan['too_late_at']}   (rejected.reason = too_late)")
    print("\nExpect in aviation.aircraft_telemetry (70 rows):")
    for kind, detail in plan["main"].items():
        print(f"  {kind:<16} {detail}")
    print("\nExpect in aviation.aircraft_telemetry_rejected (30 rows):")
    for reason, count in plan["rejected"].items():
        print(f"  {count:>2}  {reason}")
    print(
        """
Wait for the next Flink checkpoint (about 30s), then run:

SELECT kind,
       COUNT(*) AS cnt,
       SUM(CASE WHEN overheat THEN 1 ELSE 0 END) AS overheat_true,
       SUM(CASE WHEN late THEN 1 ELSE 0 END) AS late_true
FROM (
    SELECT
        CASE
            WHEN event_id LIKE 'BATCH-%(token)s-LH-%%' THEN 'late_overheat'
            WHEN event_id LIKE 'BATCH-%(token)s-L-%%' THEN 'late'
            WHEN event_id LIKE 'BATCH-%(token)s-H-%%' THEN 'overheat'
            WHEN event_id LIKE 'BATCH-%(token)s-N-%%' THEN 'normal'
            ELSE 'other'
        END AS kind,
        overheat,
        late
    FROM aviation.aircraft_telemetry
    WHERE aircraft_id = '%(aircraft_id)s'
) t
GROUP BY kind
ORDER BY kind;

SELECT reason, COUNT(*) AS cnt
FROM aviation.aircraft_telemetry_rejected
WHERE aircraft_id = '%(aircraft_id)s'
   OR event_id LIKE 'BATCH-%(token)s-NOAC-%%'
GROUP BY reason
ORDER BY reason;
"""
        % {"token": token, "aircraft_id": aircraft_id}
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Send aircraft telemetry test events"
    )
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

    message = build_messages(datetime.now(timezone.utc))[args.scenario]
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )
    try:
        result = producer.send(
            TOPIC, key=message["aircraft_id"].encode("utf-8"), value=message
        ).get(timeout=10)
    finally:
        producer.close()

    print("\nSent:")
    print(json.dumps(message, indent=2))
    print(f"\npartition={result.partition}, offset={result.offset}")


if __name__ == "__main__":
    main()
