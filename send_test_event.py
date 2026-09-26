"""Send one aircraft-telemetry event to Kafka.

Pick a scenario:

    python send_test_event.py normal
    python send_test_event.py duplicate
    python send_test_event.py overheat
    python send_test_event.py watermark
    python send_test_event.py too_late

Timestamps use millisecond precision and a Z suffix so Flink's ISO-8601
parser accepts them. Send `normal` before `duplicate`. Send `watermark`
before `too_late`.
"""

import argparse
import json
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer

TOPIC = "aircraft-telemetry"


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
            "telemetry": {"altitude": 10900, "speed": 870, "engine_temp": 700},
        },
        # engine_temp > 1000. Lands in the reject table as overheat.
        "overheat": {
            "event_id": "TEST-OVERHEAT-001",
            "aircraft_id": "AC-202",
            "timestamp": stamp,
            "telemetry": {"altitude": 12000, "speed": 820, "engine_temp": 1100},
        },
        # Moves the watermark forward so a following too_late event can be judged.
        "watermark": {
            "event_id": "TEST-WATERMARK-002",
            "aircraft_id": "AC-404",
            "timestamp": iso_z(now + timedelta(seconds=30)),
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Send one aircraft telemetry test event")
    parser.add_argument(
        "scenario",
        choices=("normal", "duplicate", "overheat", "watermark", "too_late"),
    )
    parser.add_argument("--bootstrap", default="localhost:9092")
    args = parser.parse_args()

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
