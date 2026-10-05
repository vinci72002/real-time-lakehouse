"""Hot-aircraft skew across 12 partitions and 3 Flink subtasks.

    python tests/test_skew.py

Expects the topic to have 12 partitions and the job parallelism to be 3.
Does not change the producer key or add a second aggregation stage.
"""

from __future__ import annotations

import json
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from telemetry_event import message  # noqa: E402
from tests.cluster_io import (  # noqa: E402
    backpressure,
    job_snapshot,
    metric_ids,
    records_by_subtask,
    source_records_by_subtask,
    source_vertex_id,
    vertex_id_containing,
    vertices,
)

TOPIC = "aircraft-telemetry"
TOTAL = 60_000
HOT = "AC-S00"
HOT_COUNT = 24_000


def _offsets() -> dict[int, int]:
    completed = subprocess.run(
        [
            "docker",
            "exec",
            "kafka",
            "/opt/kafka/bin/kafka-get-offsets.sh",
            "--bootstrap-server",
            "localhost:9092",
            "--topic",
            TOPIC,
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    text = (completed.stdout or "") + "\n" + (completed.stderr or "")
    if completed.returncode != 0:
        raise RuntimeError(text[-2000:])
    found = {}
    for line in text.splitlines():
        parts = line.strip().split(":")
        if len(parts) == 3 and parts[0] == TOPIC:
            found[int(parts[1])] = int(parts[2])
    return found


def main() -> int:
    from kafka import KafkaProducer

    token = uuid.uuid4().hex[:6]
    flight_id = f"SKEW-{token}"
    aircraft = [f"AC-S{index:02d}" for index in range(100)]
    others = aircraft[1:]
    plan = [HOT] * HOT_COUNT
    base, extra = divmod(TOTAL - HOT_COUNT, len(others))
    for index, aircraft_id in enumerate(others):
        plan.extend([aircraft_id] * (base + (1 if index < extra else 0)))
    if len(plan) != TOTAL or plan.count(HOT) != HOT_COUNT:
        raise RuntimeError(f"plan size {len(plan)} hot {plan.count(HOT)}")

    before = _offsets()
    print(f"partitions before: {len(before)} offsets={before}")
    job = job_snapshot()
    job_id = job["jid"]
    source_before = source_records_by_subtask(job_id)
    print(f"source subtasks before: {source_before}")

    source_id = source_vertex_id(job_id)
    window_id = vertex_id_containing(job_id, "GlobalWindowAggregate")
    samples: list[tuple[str, dict]] = []
    stop_sampling = threading.Event()

    def sample_backpressure() -> None:
        while not stop_sampling.is_set():
            for label, vertex_id in (("source", source_id), ("window", window_id)):
                try:
                    samples.append((label, backpressure(job_id, vertex_id)))
                except Exception as exc:  # noqa: BLE001
                    samples.append((label, {"error": str(exc)}))
            stop_sampling.wait(2)

    sampler = threading.Thread(target=sample_backpressure, daemon=True)
    sampler.start()
    # Sampling continues through the drain, not only the Kafka send.
    started = time.time()
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        linger_ms=20,
        batch_size=64 * 1024,
    )
    when = datetime.now(timezone.utc)
    try:
        for index, aircraft_id in enumerate(plan):
            producer.send(
                TOPIC,
                key=aircraft_id.encode("utf-8"),
                value=message(
                    f"SKEW-{token}-{index:06d}",
                    aircraft_id,
                    flight_id,
                    when,
                ),
            )
        producer.flush()
    finally:
        producer.close()
    elapsed = time.time() - started
    print(f"sent {TOTAL} in {elapsed:.1f}s ({TOTAL / elapsed:.0f}/s)")
    print(f"backpressure samples: {len(samples)}")
    for label, sample in samples:
        status = sample.get("status") or sample.get("backpressure-level") or sample
        print(f"  {label}: {status}")

    deadline = time.time() + 600
    after_source = source_before
    while time.time() < deadline:
        after_source = source_records_by_subtask(job_id)
        gained = sum(after_source) - sum(source_before)
        print(f"source delta={gained} by subtask={after_source}")
        if gained >= TOTAL:
            break
        time.sleep(5)
    stop_sampling.set()
    sampler.join(timeout=5)

    after = _offsets()
    deltas = {
        partition: after.get(partition, 0) - before.get(partition, 0)
        for partition in sorted(set(before) | set(after))
    }
    print("\nKafka partition deltas:")
    for partition, count in deltas.items():
        if count:
            print(f"  p{partition:02d}  {count:6d}  {count / TOTAL:.1%}")
    source_delta = [value - source_before[index] for index, value in enumerate(after_source)]
    print(f"source subtask deltas: {source_delta}")
    for vertex in vertices(job_id):
        if "Window" not in vertex["name"] and "Deduplicate" not in vertex["name"]:
            continue
        names = [
            name
            for name in metric_ids(job_id, vertex["id"], 0)
            if name.endswith(".numRecordsIn") and "PerSecond" not in name
        ]
        for name in names:
            counts = records_by_subtask(job_id, vertex["id"], name)
            total = sum(counts)
            share = f"{max(counts) / total:.1%}" if total else "n/a"
            print(f"{vertex['name'][:80]}")
            print(f"  {name}: {counts} busiest={share}")
    busiest = max(deltas.values()) if deltas else 0
    print(f"busiest partition share: {busiest / TOTAL:.1%}")
    if source_delta and sum(source_delta):
        print(f"busiest source subtask share: {max(source_delta) / sum(source_delta):.1%}")
    consume_elapsed = time.time() - started
    print(f"source caught up in {consume_elapsed:.1f}s from send start")
    return 0


if __name__ == "__main__":
    sys.exit(main())
