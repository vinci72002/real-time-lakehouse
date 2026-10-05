"""
Aircraft telemetry producer.

Simulates three aircraft on city-pair routes and prints each sensor event.

- One event every 0.5 seconds (configurable)
- About 5% of events are late, out-of-order, too-late, or a reused event_id
- About 3% set engine_temp_c above 1000 so the downstream job flags overheat
- Position, altitude, speed, vertical speed, and fuel move together by flight phase
- Writes a local JSONL file, and also publishes to Kafka when it is reachable
"""

from __future__ import annotations

import argparse
import json
import os
import random
import signal
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.theme import Theme

from telemetry_event import FlightTrack
from rich.text import Text


def _configure_stdio() -> None:
    """Windows consoles default to a legacy code page. Force UTF-8."""
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")


_configure_stdio()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ROUTES = {
    "AC-101": ((31.144, 121.805), (40.080, 116.585)),  # Shanghai - Beijing
    "AC-102": ((22.309, 113.922), (31.144, 121.805)),  # Hong Kong - Shanghai
    "AC-103": ((30.578, 103.947), (23.392, 113.299)),  # Chengdu - Guangzhou
}
DEFAULT_INTERVAL = 0.5
DEFAULT_ANOMALY_RATE = 0.05
DEFAULT_HOT_RATE = 0.03
KAFKA_TOPIC = "aircraft-telemetry"
LOCAL_SINK = Path("data/telemetry.jsonl")

AIRCRAFT_STYLE = {
    "AC-101": "bold bright_cyan",
    "AC-102": "bold bright_green",
    "AC-103": "bold bright_magenta",
}

THEME = Theme(
    {
        "banner": "bold white",
        "ok": "bold bright_green",
        "late": "bold yellow",
        "dup": "bold bright_magenta",
        "hot": "bold bright_red",
        "dim": "dim",
        "kafka": "bold bright_blue",
        "stat": "bold cyan",
    }
)

console = Console(theme=THEME, highlight=False, legacy_windows=False)


# ---------------------------------------------------------------------------
# Fleet
# ---------------------------------------------------------------------------

AIRCRAFT_IDS = tuple(ROUTES)


def _fleet() -> list[FlightTrack]:
    now = datetime.now(timezone.utc)
    return [
        FlightTrack(
            aircraft_id,
            origin,
            dest,
            started=now - timedelta(seconds=offset),
            elapsed_s=offset,
        )
        for offset, (aircraft_id, (origin, dest)) in zip(
            (400, 1400, 2800),
            ROUTES.items(),
        )
    ]


# ---------------------------------------------------------------------------
# Kafka, optional
# ---------------------------------------------------------------------------


class OptionalKafka:
    """Publish when Kafka is up. Otherwise keep the local demo running."""

    def __init__(self, bootstrap: str, topic: str) -> None:
        self.bootstrap = bootstrap
        self.topic = topic
        self.producer = None
        self.ok = False
        self.sent = 0
        self.errors = 0

    def connect(self) -> None:
        try:
            from kafka import KafkaProducer  # type: ignore
        except ImportError:
            console.print("[dim]kafka-python is not installed; writing JSONL and console output only[/]")
            return

        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap,
                value_serializer=lambda v: v.encode("utf-8"),
                key_serializer=lambda v: v.encode("utf-8"),
                linger_ms=50,
                retries=1,
                request_timeout_ms=2_000,
                max_block_ms=2_000,
                api_version=(2, 8, 0),
            )
            # One metadata lookup so a half-open connection is not treated as success.
            self.producer.partitions_for(self.topic)
            self.ok = True
        except Exception as exc:  # noqa: BLE001
            self.producer = None
            self.ok = False
            console.print(f"[dim]Kafka unavailable ({self.bootstrap}): {exc}[/]")
            console.print("[dim]Falling back to local JSONL. The simulated job tails that file.[/]")

    def send(self, payload: dict) -> None:
        if not self.ok or self.producer is None:
            return
        try:
            self.producer.send(
                self.topic,
                key=payload["aircraft_id"],
                value=json.dumps(payload),
            )
            self.sent += 1
        except Exception:  # noqa: BLE001
            self.errors += 1
            self.ok = False

    def close(self) -> None:
        if self.producer is not None:
            try:
                self.producer.flush(timeout=2)
                self.producer.close(timeout=2)
            except Exception:  # noqa: BLE001
                pass


# ---------------------------------------------------------------------------
# Event factory
# ---------------------------------------------------------------------------


@dataclass
class GeneratorStats:
    total: int = 0
    normal: int = 0
    late: int = 0
    duplicate: int = 0
    overheat: int = 0
    per_ac: dict[str, int] = field(default_factory=lambda: {a: 0 for a in AIRCRAFT_IDS})


def build_event(
    aircraft: FlightTrack,
    anomaly_rate: float,
    hot_rate: float,
    stats: GeneratorStats,
) -> tuple[dict, Optional[str]]:
    """Advance one flight sample and occasionally inject an anomaly.

    The anomaly tag is for the console only. It is not written to Kafka.
    Timestamp shifts are classified by Flink, not by this tag:

    - 2 to 4 seconds back: inside the 5-second watermark (out of order)
    - 8 to 14 seconds back: late on the detail table, dropped by the window
    - 60 seconds back: too late, rejected
    """
    previous = aircraft.last_message
    payload = aircraft.advance(1.0)
    anomaly: Optional[str] = None

    roll = random.random()
    if roll < anomaly_rate and previous is not None:
        kind = random.random()
        if kind < 0.4:
            payload = dict(previous)
            aircraft.last_message = previous
            anomaly = "duplicate"
            stats.duplicate += 1
        else:
            raw = payload["event_time"].replace("Z", "+00:00")
            event_time = datetime.fromisoformat(raw)
            if kind < 0.6:
                payload["event_time"] = (
                    event_time - timedelta(seconds=random.uniform(2, 4))
                ).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
                anomaly = "out_of_order"
                stats.late += 1
            elif kind < 0.9:
                payload["event_time"] = (
                    event_time - timedelta(seconds=random.uniform(8, 14))
                ).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
                anomaly = "late"
                stats.late += 1
            else:
                payload["event_time"] = (
                    event_time - timedelta(seconds=60)
                ).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
                anomaly = "too_late"
                stats.late += 1
            payload["event_id"] = str(uuid.uuid4())
    else:
        stats.normal += 1

    if anomaly != "duplicate" and random.random() < hot_rate:
        payload["engine_temp_c"] = round(random.uniform(1050, 1200), 1)
        payload["fuel_flow_kg_h"] = 3600.0
        anomaly = "overheat" if anomaly is None else f"{anomaly}+overheat"
        stats.overheat += 1

    if anomaly != "duplicate":
        aircraft.last_message = payload

    stats.total += 1
    stats.per_ac[aircraft.aircraft_id] += 1
    return payload, anomaly


# ---------------------------------------------------------------------------
# Console output
# ---------------------------------------------------------------------------


def print_banner(interval: float, kafka: OptionalKafka) -> None:
    sink = "Kafka + JSONL" if kafka.ok else "local JSONL (Kafka offline)"
    body = Text.from_markup(
        "[banner]Aircraft telemetry lakehouse  ·  Producer[/]\n"
        f"[dim]fleet[/]  AC-101   AC-102   AC-103\n"
        f"[dim]rate[/]   one event every [stat]{interval:.1f}s[/]   "
        f"[dim]anomaly[/]  5% duplicate/time-shift   [dim]dirty[/]  3% overheat\n"
        f"[dim]sink[/]   {sink}   [dim]topic[/]  {KAFKA_TOPIC}"
    )
    console.print(
        Panel(
            body,
            title="[ok]>> TELEMETRY PRODUCER[/]",
            subtitle="[dim]Ctrl+C to stop[/]",
            border_style="bright_cyan",
            box=box.DOUBLE,
            padding=(1, 2),
        )
    )
    console.print()


def _badge(anomaly: Optional[str]) -> Text:
    if anomaly is None:
        return Text(" NORMAL ", style="black on bright_green")
    if "duplicate" in anomaly:
        return Text("  DUP   ", style="black on bright_magenta")
    if anomaly == "too_late" or anomaly.startswith("too_late"):
        return Text("  LATE+ ", style="black on yellow")
    if "out_of_order" in anomaly:
        return Text("  OOO   ", style="black on yellow")
    if "late" in anomaly:
        return Text("  LATE  ", style="black on yellow")
    if "overheat" in anomaly:
        return Text("  HOT   ", style="white on bright_red")
    return Text(f" {anomaly.upper()} ", style="white on red")


def print_event(payload: dict, anomaly: Optional[str]) -> None:
    ac_style = AIRCRAFT_STYLE.get(payload["aircraft_id"], "white")
    wall = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    event_ts = payload["event_time"][11:23]

    line = Text()
    line.append(f" {wall} ", style="dim")
    line.append(f" {payload['aircraft_id']} ", style=f"black on {ac_style.split()[-1]}")
    line.append("  ")
    line.append_text(_badge(anomaly))
    line.append("  ")
    line.append(f"alt={payload['altitude_ft']:>8,.0f} ft", style="bright_white")
    line.append("   ")
    line.append(f"gs={payload['ground_speed_kts']:>6.0f} kt", style="bright_white")
    line.append("   ")
    line.append(f"vs={payload['vertical_speed_fpm']:>7.0f}", style="bright_white")
    temp = payload["engine_temp_c"]
    temp_style = "hot" if temp > 1000 else "bright_white"
    line.append(f"  egt={temp:>6.0f} C", style=temp_style)
    line.append("   ")
    line.append(f"fuel={payload['fuel_remaining_kg']:>8,.0f} kg", style="bright_white")
    line.append("   ")
    line.append(f"id={payload['event_id'][:8]}", style="dim")
    if anomaly in ("late", "out_of_order", "too_late"):
        line.append(f"   event_time={event_ts}", style="late")
    console.print(line)
    console.print(f"   [dim]{json.dumps(payload, ensure_ascii=False)}[/]")


def print_stats(stats: GeneratorStats, kafka: OptionalKafka) -> None:
    table = Table(
        box=box.SIMPLE_HEAVY,
        show_header=True,
        header_style="bold cyan",
        title="[stat]── producer snapshot ──[/]",
        title_style="stat",
        expand=False,
    )
    table.add_column("metric", style="dim")
    table.add_column("value", justify="right", style="bold white")
    table.add_row("total", str(stats.total))
    table.add_row("normal", f"{stats.normal}")
    table.add_row("late (ooo)", f"{stats.late}")
    table.add_row("duplicate", f"{stats.duplicate}")
    table.add_row("overheat", f"{stats.overheat}")
    for ac_id, count in stats.per_ac.items():
        table.add_row(ac_id, str(count))
    table.add_row("kafka sent", str(kafka.sent) if kafka.ok else "offline")
    console.print(table)
    console.print()


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Aircraft telemetry producer")
    parser.add_argument("--interval", type=float, default=DEFAULT_INTERVAL, help="Seconds between events")
    parser.add_argument("--anomaly-rate", type=float, default=DEFAULT_ANOMALY_RATE)
    parser.add_argument("--hot-rate", type=float, default=DEFAULT_HOT_RATE)
    parser.add_argument(
        "--bootstrap",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap servers",
    )
    parser.add_argument("--no-kafka", action="store_true", help="Do not connect to Kafka")
    parser.add_argument("--jsonl", type=Path, default=LOCAL_SINK, help="Local JSONL path")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    stats = GeneratorStats()
    fleet = _fleet()
    running = True

    def _stop(signum, _frame):  # noqa: ARG001
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, _stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _stop)

    kafka = OptionalKafka(args.bootstrap, KAFKA_TOPIC)
    if not args.no_kafka:
        kafka.connect()

    args.jsonl.parent.mkdir(parents=True, exist_ok=True)
    print_banner(args.interval, kafka)

    try:
        with args.jsonl.open("a", encoding="utf-8") as sink:
            while running:
                aircraft = random.choice(fleet)
                payload, anomaly = build_event(
                    aircraft, args.anomaly_rate, args.hot_rate, stats
                )
                line = json.dumps(payload)
                sink.write(line + "\n")
                sink.flush()
                kafka.send(payload)
                print_event(payload, anomaly)

                if stats.total % 20 == 0:
                    print_stats(stats, kafka)

                time.sleep(max(args.interval, 0.05))
    except KeyboardInterrupt:
        running = False
    finally:
        kafka.close()
        console.print()
        print_stats(stats, kafka)
        console.print("[ok]producer stopped[/]")
    return 0


if __name__ == "__main__":
    sys.exit(main())
