"""Send 100,000 unique events and kill the TaskManager mid-consumption.

    python tests/test_recovery_100k.py

The JobManager stays up. The job must restart from the latest checkpoint.
"""

from __future__ import annotations

import json
import subprocess
import sys
import threading
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

from telemetry_event import message  # noqa: E402
from tests.cluster_io import (  # noqa: E402
    completed_checkpoints,
    job_snapshot,
    source_records,
    spark_rows,
    _get,
)

TOPIC = "aircraft-telemetry"
COUNT = 100_000


def main() -> int:
    from kafka import KafkaProducer

    token = uuid.uuid4().hex[:6]
    flight_id = f"REC-{token}"
    ids = [f"REC-{token}-{index:06d}" for index in range(COUNT)]
    id_path = ROOT / "data" / f"recovery-{token}.ids"
    id_path.parent.mkdir(parents=True, exist_ok=True)
    id_path.write_text("\n".join(ids), encoding="utf-8")
    print(f"recovery token={token} ids={id_path}")

    job = job_snapshot()
    job_id = job["jid"]
    start_records = source_records(job_id)
    start_checkpoints = completed_checkpoints(job_id)
    when = datetime.now(timezone.utc) + timedelta(minutes=3)
    killed = {"done": False, "records": None, "checkpoints": None}

    def produce() -> None:
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            acks="all",
            linger_ms=20,
            batch_size=64 * 1024,
        )
        try:
            for index, event_id in enumerate(ids):
                aircraft_id = f"AC-{index % 10:03d}"
                producer.send(
                    TOPIC,
                    key=aircraft_id.encode("utf-8"),
                    value=message(event_id, aircraft_id, flight_id, when),
                )
                if index % 200 == 199:
                    producer.flush()
                    time.sleep(0.08)
            producer.flush()
        finally:
            producer.close()

    sender = threading.Thread(target=produce, daemon=True)
    sender.start()
    deadline = time.time() + 180
    while time.time() < deadline and sender.is_alive():
        try:
            consumed = source_records(job_id) - start_records
            checkpoints = completed_checkpoints(job_id)
        except Exception as exc:  # noqa: BLE001
            print(f"metrics unavailable while sending: {exc}")
            time.sleep(2)
            continue
        print(f"consumed_delta={consumed} checkpoints={checkpoints}")
        if (
            not killed["done"]
            and consumed >= 20_000
            and checkpoints > start_checkpoints
        ):
            print(f"killing TaskManager at consumed_delta={consumed}")
            history = json.loads(_get(f"http://localhost:8081/jobs/{job_id}/checkpoints"))
            latest = history.get("latest", {}).get("completed")
            print(f"latest completed checkpoint: {latest}")
            subprocess.run(["docker", "kill", "flink-taskmanager"], check=True)
            killed["done"] = True
            killed["records"] = consumed
            killed["checkpoints"] = checkpoints
            killed["checkpoint"] = latest
        time.sleep(2)

    sender.join()
    if not killed["done"]:
        consumed = source_records(job_id) - start_records
        print(f"send finished before the kill window, consumed_delta={consumed}")
        subprocess.run(["docker", "kill", "flink-taskmanager"], check=True)
        killed["records"] = consumed
        killed["checkpoints"] = completed_checkpoints(job_id)

    print("starting TaskManager")
    subprocess.run(["docker", "start", "flink-taskmanager"], check=True)

    recovered = None
    deadline = time.time() + 180
    while time.time() < deadline:
        try:
            snapshot = job_snapshot()
        except RuntimeError:
            snapshot = None
        print(f"job after kill: {snapshot}")
        if snapshot and snapshot.get("state") == "RUNNING" and snapshot.get("jid") == job_id:
            recovered = snapshot
            break
        time.sleep(5)
    if recovered is None:
        print("FAIL  job did not return to RUNNING on the same job id")
        return 1

    previous = -1
    stable = 0
    visible = 0
    # Parallelism 1 drains slower than the producer. Iceberg only shows
    # rows after a checkpoint, so a short wait looks like data loss.
    deadline = time.time() + 900
    while time.time() < deadline:
        count_rows = spark_rows(
            "SELECT CAST(COUNT(1) AS STRING) FROM demo.aviation.aircraft_telemetry "
            f"WHERE flight_id = '{flight_id}'"
        )
        visible = int(count_rows[0][0]) if count_rows else 0
        print(f"iceberg rows for {flight_id}: {visible}", flush=True)
        if visible == previous and visible > 0:
            stable += 1
            if stable >= 2 and visible >= COUNT:
                break
        else:
            stable = 0
        previous = visible
        time.sleep(30)
    rows = spark_rows(
        "SELECT event_id FROM demo.aviation.aircraft_telemetry "
        f"WHERE flight_id = '{flight_id}'"
    )
    iceberg_ids = [row[0] for row in rows]

    produced = set(ids)
    found = iceberg_ids
    unique = set(found)
    missing = produced - unique
    unexpected = unique - produced
    duplicates = len(found) - len(unique)
    print("\nRecovery comparison")
    print(f"  kill consumed_delta={killed['records']} checkpoints_seen={killed['checkpoints']}")
    print(f"  produced={len(produced)}")
    print(f"  iceberg_rows={len(found)}")
    print(f"  iceberg_unique={len(unique)}")
    print(f"  missing={len(missing)}")
    print(f"  unexpected={len(unexpected)}")
    print(f"  duplicate_rows={duplicates}")
    if missing:
        print("  missing sample:", ", ".join(sorted(missing)[:5]))
    if unexpected:
        print("  unexpected sample:", ", ".join(sorted(unexpected)[:5]))
    ok = not missing and not unexpected and duplicates == 0 and len(found) == COUNT
    print("PASS" if ok else "FAIL", "  100k id comparison")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
