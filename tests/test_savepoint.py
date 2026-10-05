"""Savepoint stop/restore for dedup state, window state, and Kafka offsets.

    python tests/test_savepoint.py

The job must already be running. This stops it with a savepoint and submits
the same SQL from that savepoint.
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from telemetry_event import iso_z, message  # noqa: E402
from tests.cluster_io import (  # noqa: E402
    job_snapshot,
    metric,
    source_vertex_id,
    spark_rows,
)

TOPIC = "aircraft-telemetry"


def _check(name: str, ok: bool, detail: str, results: list[tuple[str, bool, str]]) -> None:
    results.append((name, ok, detail))
    print(f"{'PASS' if ok else 'FAIL'}  {name}  {detail}")


def _send(producer, aircraft_id: str, payload: dict) -> None:
    producer.send(TOPIC, key=aircraft_id.encode("utf-8"), value=payload).get(timeout=30)


def _offsets(job_id: str) -> dict[int, int]:
    vertex = source_vertex_id(job_id)
    found = {}
    for partition in range(3):
        name = (
            "Source__kafka_telemetry[1].KafkaSourceReader.topic."
            f"aircraft-telemetry.partition.{partition}.committedOffset"
        )
        try:
            found[partition] = int(float(metric(job_id, vertex, 0, name)))
        except RuntimeError:
            found[partition] = -1
    return found


def _powershell(action: str) -> str:
    completed = subprocess.run(
        ["powershell", "-NoProfile", "-File", str(ROOT / "scripts" / "savepoint.ps1"), action],
        check=False,
        capture_output=True,
        text=True,
    )
    text = (completed.stdout or "") + "\n" + (completed.stderr or "")
    print(text[-2500:])
    if completed.returncode != 0:
        raise RuntimeError(f"savepoint.ps1 {action} failed")
    return text


def _wait_job(state: str, timeout: float) -> dict:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = job_snapshot()
        except RuntimeError:
            last = None
        if last and last.get("state") == state:
            return last
        time.sleep(3)
    raise RuntimeError(f"job did not reach {state}: {last}")


def main() -> int:
    from kafka import KafkaProducer

    token = uuid.uuid4().hex[:6]
    aircraft_id = "AC-101"
    flight_id = f"AC-101-SP-{token}"
    ahead = datetime.now(timezone.utc) + timedelta(minutes=5)
    window = ahead.replace(second=0, microsecond=0) + timedelta(minutes=1)
    event_a = f"SP-{token}-A"
    event_b = f"SP-{token}-B"
    event_c = f"SP-{token}-C"
    print(f"savepoint token={token} window={iso_z(window)} flight={flight_id}")

    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
    )
    try:
        _send(producer, aircraft_id, message(event_a, aircraft_id, flight_id, window + timedelta(seconds=10)))
        _send(producer, aircraft_id, message(event_b, aircraft_id, flight_id, window + timedelta(seconds=25)))
    finally:
        producer.close()

    deadline = time.time() + 90
    seen: set[str] = set()
    while time.time() < deadline:
        rows = spark_rows(
            "SELECT event_id FROM demo.aviation.aircraft_telemetry "
            f"WHERE flight_id = '{flight_id}'"
        )
        seen = {row[0] for row in rows}
        if event_a in seen and event_b in seen:
            break
        print(f"waiting for checkpoint before savepoint, seen={sorted(seen)}")
        time.sleep(10)
    if event_a not in seen or event_b not in seen:
        print("FAIL  events were not checkpointed before the savepoint")
        return 1

    before = job_snapshot()
    offsets_before = _offsets(before["jid"])
    print(f"offsets before stop: {offsets_before}")
    _powershell("stop")
    _powershell("resume")
    restored = _wait_job("RUNNING", 180)
    print(f"restored job {restored['jid']}")
    time.sleep(5)
    offsets_after = _offsets(restored["jid"])
    print(f"offsets after restore: {offsets_after}")

    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
    )
    try:
        _send(producer, aircraft_id, message(event_a, aircraft_id, flight_id, window + timedelta(seconds=10)))
        _send(
            producer,
            aircraft_id,
            message(event_c, aircraft_id, flight_id, window + timedelta(seconds=80)),
        )
    finally:
        producer.close()

    deadline = time.time() + 120
    detail: dict[str, int] = {}
    rejected: dict[str, str] = {}
    windows: list[tuple[str, str]] = []
    minute = window.strftime("%Y-%m-%d %H:%M:%S")
    while time.time() < deadline:
        detail_rows = spark_rows(
            "SELECT event_id FROM demo.aviation.aircraft_telemetry "
            f"WHERE flight_id = '{flight_id}'"
        )
        detail = {}
        for row in detail_rows:
            detail[row[0]] = detail.get(row[0], 0) + 1
        rejected = {
            row[0]: row[1]
            for row in spark_rows(
                "SELECT event_id, reason FROM demo.aviation.aircraft_telemetry_rejected "
                f"WHERE event_id LIKE 'SP-{token}-%'"
            )
            if len(row) >= 2
        }
        windows = [
            (row[0], row[1])
            for row in spark_rows(
                "SELECT CAST(window_start AS STRING), CAST(event_count AS STRING) "
                "FROM demo.aviation.aircraft_telemetry_1m "
                f"WHERE flight_id = '{flight_id}'"
            )
            if len(row) >= 2
        ]
        closed = [count for start, count in windows if start.startswith(minute)]
        if detail.get(event_a) == 1 and rejected.get(event_a) == "duplicate" and closed:
            break
        print(f"waiting after restore detail={detail} rejected={rejected} windows={windows}")
        time.sleep(10)

    results: list[tuple[str, bool, str]] = []
    _check(
        "kafka offsets were not rewound",
        offsets_after.get(0, -1) >= offsets_before.get(0, 0) > 0,
        f"before={offsets_before} after={offsets_after}",
        results,
    )
    _check(
        "original event stays a single detail row",
        detail.get(event_a) == 1 and detail.get(event_b) == 1,
        f"detail={detail}",
        results,
    )
    _check(
        "replayed event_id is rejected as duplicate",
        rejected.get(event_a) == "duplicate",
        f"rejected={rejected}",
        results,
    )
    closed_count = next((count for start, count in windows if start.startswith(minute)), None)
    _check(
        "window state kept the two pre-savepoint events",
        closed_count == "2",
        f"minute={minute} windows={windows}",
        results,
    )
    failed = [item for item in results if not item[1]]
    print(f"\n{len(results) - len(failed)} passed, {len(failed)} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
