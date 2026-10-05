"""Deterministic checks for V2 telemetry semantics.

Local flight shape does not need the cluster. The streaming checks need
Kafka, the running Flink job, and Spark.

    python tests/test_semantics.py
    python tests/test_semantics.py --local-only

Streaming timeline, all on one aircraft so Kafka order is preserved.
W is a minute boundary several minutes ahead of wall time.

    send together:
        OT1 at W+10s, OT2 at W+30s, duplicate of OT1,
        OOH at W+50s, OOL at W+47s
    wait 15s (idle partitions release the watermark; watermark ~= W+45s)
    send LATE at W+37s
        behind that watermark, inside 15s, and inside the still-open minute
    send CLOSE at W+70s
        watermark becomes W+65s, so minute [W, W+60s) closes
    wait 15s
    send PC at W+55s
        inside the closed minute, 10s behind the watermark
    send TL at W+5s
        more than 15s behind the watermark

Expected, measured on Flink 1.19:

    detail keeps OT1, OT2, OOH, OOL, LATE, CLOSE, PC
    LATE and PC have late = true
    OOL has late = false
    duplicate and TL are rejected
    minute [W, W+60s) event_count = 5 and late_count = 1
        OT1, OT2, OOH, OOL, and LATE (the window was still open)
        PC arrives after that minute has closed, so it is not added
    minute [W+60s, W+120s) stays unemitted: watermark has not passed its end
"""

from __future__ import annotations

import argparse
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

from telemetry_event import FLIGHT_DURATION_S, FlightTrack, iso_z, sample_message  # noqa: E402

TOPIC = "aircraft-telemetry"


def _check(name: str, ok: bool, detail: str, results: list[tuple[str, bool, str]]) -> None:
    results.append((name, ok, detail))
    print(f"{'PASS' if ok else 'FAIL'}  {name}  {detail}")


def local_flight_shape() -> int:
    started = datetime(2026, 10, 5, 8, 0, tzinfo=timezone.utc)
    track = FlightTrack(
        "AC-017",
        (31.144, 121.805),
        (40.080, 116.585),
        started=started,
    )
    marks = (60, 200, 900, 2000, 3600, 4300, FLIGHT_DURATION_S - 1)
    rows = []
    previous_fuel = None
    results: list[tuple[str, bool, str]] = []
    print("\nSample flight AC-017 Shanghai -> Beijing")
    for elapsed in marks:
        row = sample_message(
            event_id=f"sample-{int(elapsed)}",
            aircraft_id=track.aircraft_id,
            flight_id=track.flight_id,
            event_time=started + timedelta(seconds=elapsed),
            elapsed_s=elapsed,
            origin=track.origin,
            dest=track.dest,
        )
        rows.append(row)
        print(
            f"  t={elapsed:5.0f}s  {row['event_time']}  "
            f"alt={row['altitude_ft']:8.0f}  gs={row['ground_speed_kts']:6.0f}  "
            f"vs={row['vertical_speed_fpm']:7.0f}  "
            f"fuel={row['fuel_remaining_kg']:8.0f}  "
            f"egt={row['engine_temp_c']:6.0f}  "
            f"pos=({row['latitude']:.3f},{row['longitude']:.3f})"
        )
        if previous_fuel is not None:
            _check(
                f"fuel decreases by t={int(elapsed)}",
                row["fuel_remaining_kg"] < previous_fuel,
                f"{previous_fuel} -> {row['fuel_remaining_kg']}",
                results,
            )
        previous_fuel = row["fuel_remaining_kg"]

    climb = rows[2]
    cruise = rows[3]
    descent = rows[4]
    _check(
        "climb vertical speed is positive",
        climb["vertical_speed_fpm"] > 0 and climb["altitude_ft"] > rows[0]["altitude_ft"],
        f"vs={climb['vertical_speed_fpm']} alt={climb['altitude_ft']}",
        results,
    )
    _check(
        "cruise is level and faster than climb",
        abs(cruise["vertical_speed_fpm"]) < 1
        and cruise["ground_speed_kts"] > climb["ground_speed_kts"],
        f"vs={cruise['vertical_speed_fpm']} gs={cruise['ground_speed_kts']}",
        results,
    )
    _check(
        "descent vertical speed is negative",
        descent["vertical_speed_fpm"] < 0 and descent["altitude_ft"] < cruise["altitude_ft"],
        f"vs={descent['vertical_speed_fpm']} alt={descent['altitude_ft']}",
        results,
    )
    _check(
        "position moves toward the destination",
        rows[-1]["latitude"] > rows[0]["latitude"],
        f"{rows[0]['latitude']} -> {rows[-1]['latitude']}",
        results,
    )
    failed = [item for item in results if not item[1]]
    return 1 if failed else 0


def _window_start(now: datetime) -> datetime:
    ahead = now + timedelta(minutes=5)
    base = ahead.replace(second=0, microsecond=0)
    return base + timedelta(minutes=1)


def _send(producer, aircraft_id: str, payload: dict) -> None:
    producer.send(TOPIC, key=aircraft_id.encode("utf-8"), value=payload)


def _spark(sql: str) -> str:
    completed = subprocess.run(
        ["docker", "exec", "spark-iceberg", "spark-sql", "-e", sql],
        check=False,
        capture_output=True,
        text=True,
    )
    text = (completed.stdout or "") + "\n" + (completed.stderr or "")
    if completed.returncode != 0:
        raise RuntimeError(text[-2000:])
    return text


def _rows(sql: str) -> list[list[str]]:
    raw = _spark(sql)
    parsed: list[list[str]] = []
    for line in raw.splitlines():
        stripped = line.strip()
        if not stripped or "\t" not in stripped:
            continue
        if any(
            stripped.startswith(prefix)
            for prefix in ("Setting ", "To adjust", "SLF4J", "WARNING", "ivy", "Spark ")
        ):
            continue
        cells = [cell.strip() for cell in stripped.split("\t")]
        if cells and cells[0].lower() in {"event_id", "reason", "window_start"}:
            continue
        parsed.append(cells)
    return parsed


def streaming_semantics(bootstrap: str) -> int:
    from kafka import KafkaProducer

    token = uuid.uuid4().hex[:6]
    aircraft_id = "AC-101"
    flight_id = f"AC-101-SEM-{token}"
    window = _window_start(datetime.now(timezone.utc))
    print(f"\nstreaming token={token} window_start={iso_z(window)} flight_id={flight_id}")

    def row(event_id: str, when: datetime, **overrides) -> dict:
        from telemetry_event import message

        return message(event_id, aircraft_id, flight_id, when, **overrides)

    ids = {
        "ot1": f"SEM-{token}-OT1",
        "ot2": f"SEM-{token}-OT2",
        "ooh": f"SEM-{token}-OOH",
        "ool": f"SEM-{token}-OOL",
        "late": f"SEM-{token}-LATE",
        "close": f"SEM-{token}-CLOSE",
        "pc": f"SEM-{token}-PC",
        "tl": f"SEM-{token}-TL",
    }
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
    )
    try:
        _send(producer, aircraft_id, row(ids["ot1"], window + timedelta(seconds=10)))
        _send(producer, aircraft_id, row(ids["ot2"], window + timedelta(seconds=30)))
        _send(producer, aircraft_id, row(ids["ot1"], window + timedelta(seconds=10)))
        _send(
            producer,
            aircraft_id,
            row(ids["ooh"], window + timedelta(seconds=50), ground_speed_kts=470),
        )
        _send(
            producer,
            aircraft_id,
            row(ids["ool"], window + timedelta(seconds=47), ground_speed_kts=468),
        )
        producer.flush()
        print("sent on-time, duplicate, and out-of-order; waiting 15s for the watermark")
        time.sleep(15)
        _send(
            producer,
            aircraft_id,
            row(ids["late"], window + timedelta(seconds=37), altitude_ft=34000),
        )
        _send(
            producer,
            aircraft_id,
            row(ids["close"], window + timedelta(seconds=70), altitude_ft=35000),
        )
        producer.flush()
        print("sent late and window-closing events; waiting 15s")
        time.sleep(15)
        _send(
            producer,
            aircraft_id,
            row(ids["pc"], window + timedelta(seconds=55), altitude_ft=34900),
        )
        _send(
            producer,
            aircraft_id,
            row(ids["tl"], window + timedelta(seconds=5), altitude_ft=30000),
        )
        producer.flush()
    finally:
        producer.close()

    detail_sql = f"""
        SELECT event_id, CAST(late AS STRING)
        FROM demo.aviation.aircraft_telemetry
        WHERE flight_id = '{flight_id}'
    """
    reject_sql = f"""
        SELECT event_id, reason
        FROM demo.aviation.aircraft_telemetry_rejected
        WHERE flight_id = '{flight_id}'
           OR event_id LIKE 'SEM-{token}-%'
    """
    window_sql = f"""
        SELECT CAST(window_start AS STRING),
               CAST(event_count AS STRING),
               CAST(late_count AS STRING)
        FROM demo.aviation.aircraft_telemetry_1m
        WHERE flight_id = '{flight_id}'
    """

    deadline = time.time() + 180
    detail: dict[str, str] = {}
    rejected: dict[str, str] = {}
    windows: list[tuple[str, str]] = []
    while time.time() < deadline:
        detail = {row[0]: row[1].lower() for row in _rows(detail_sql) if len(row) >= 2}
        rejected = {row[0]: row[1] for row in _rows(reject_sql) if len(row) >= 2}
        windows = [(row[0], row[1], row[2] if len(row) > 2 else "") for row in _rows(window_sql) if len(row) >= 2]
        if ids["pc"] in detail and ids["tl"] in rejected and len(windows) >= 1:
            break
        print(
            f"waiting for checkpoint  detail={len(detail)} "
            f"rejected={len(rejected)} windows={len(windows)}"
        )
        time.sleep(10)

    print("\nDetail rows:")
    for event_id, late in sorted(detail.items()):
        print(f"  {event_id}  late={late}")
    print("Rejected rows:")
    for event_id, reason in sorted(rejected.items()):
        print(f"  {event_id}  reason={reason}")
    print("Windows:")
    for start, count, late_count in windows:
        print(f"  {start}  event_count={count}  late_count={late_count}")

    results: list[tuple[str, bool, str]] = []
    kept = [ids["ot1"], ids["ot2"], ids["ooh"], ids["ool"], ids["late"], ids["close"], ids["pc"]]
    for event_id in kept:
        _check(f"detail contains {event_id.split('-')[-1]}", event_id in detail, event_id, results)
    _check(
        "duplicate is absent from the detail table",
        list(detail).count(ids["ot1"]) == 1,
        f"copies={list(detail).count(ids['ot1'])}",
        results,
    )
    _check(
        "out-of-order row is late=false",
        detail.get(ids["ool"]) == "false",
        f"late={detail.get(ids['ool'])}",
        results,
    )
    _check(
        "behind-watermark row is late=true on the detail table",
        detail.get(ids["late"]) == "true",
        f"late={detail.get(ids['late'])}",
        results,
    )
    _check(
        "post-close row is late=true on the detail table",
        detail.get(ids["pc"]) == "true",
        f"late={detail.get(ids['pc'])}",
        results,
    )
    _check(
        "duplicate reason",
        rejected.get(ids["ot1"]) == "duplicate",
        f"reason={rejected.get(ids['ot1'])}",
        results,
    )
    _check(
        "too-late is rejected and absent from the detail table",
        rejected.get(ids["tl"]) == "too_late" and ids["tl"] not in detail,
        f"reason={rejected.get(ids['tl'])}",
        results,
    )

    first_minute = window.strftime("%Y-%m-%d %H:%M:%S")
    counts = {start: (count, late_count) for start, count, late_count in windows}

    def _for(prefix: str) -> tuple[str, str] | None:
        for start, pair in counts.items():
            if start.startswith(prefix):
                return pair
        return None

    closed = _for(first_minute)
    _check(
        "open late row is inside the minute that later closes",
        closed == ("5", "1"),
        f"got={closed} window={first_minute} all={counts}",
        results,
    )
    _check(
        "post-close row does not increase event_count or late_count",
        closed == ("5", "1"),
        "event_count 6 or late_count 2 would mean the closed minute was updated",
        results,
    )
    failed = [item for item in results if not item[1]]
    print(f"\n{len(results) - len(failed)} passed, {len(failed)} failed")
    return 1 if failed else 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--local-only", action="store_true")
    args = parser.parse_args()
    status = local_flight_shape()
    if args.local_only:
        return status
    return status or streaming_semantics(args.bootstrap)


if __name__ == "__main__":
    sys.exit(main())
