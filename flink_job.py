"""
PyFlink Core 逻辑模拟作业
========================

不依赖真实 Flink 集群，在纯 Python 中复现三条关键算子，方便本地 / OBS Demo:

  1. Watermark / 乱序处理  — 识别 event-time 落后于 watermark 的迟到数据
  2. Deduplication         — 按 event_id 去重
  3. 数据清洗              — 拦截 engine_temp > 1000 的脏数据

Source 优先级: Kafka → 本地 JSONL（由 data_generator.py 写入）
Sink   模拟写入 Iceberg 表: warehouse/iceberg/db/aircraft_telemetry/
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator, Optional

from pydantic import BaseModel, Field, ValidationError
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.theme import Theme


def _configure_stdio() -> None:
    """Windows GBK 控制台无法打印部分 Unicode，统一切到 UTF-8。"""
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")


_configure_stdio()

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------

KAFKA_TOPIC = "aircraft-telemetry"
DEFAULT_JSONL = Path("data/telemetry.jsonl")
ICEBERG_SINK = Path("warehouse/iceberg/db/aircraft_telemetry/data.jsonl")
ENGINE_TEMP_LIMIT = 1000.0
DEFAULT_OOO_SECONDS = 5.0
ALLOWED_LATENESS_SECONDS = 15.0
DEDUP_CACHE_SIZE = 2_000

THEME = Theme(
    {
        "src": "bold bright_blue",
        "wm": "bold yellow",
        "dedup": "bold bright_magenta",
        "clean": "bold bright_red",
        "sink": "bold bright_green",
        "ok": "bold bright_green",
        "dim": "dim",
        "stat": "bold cyan",
    }
)

console = Console(theme=THEME, highlight=False, legacy_windows=False)


# ---------------------------------------------------------------------------
# 数据模型
# ---------------------------------------------------------------------------


class Telemetry(BaseModel):
    altitude: float
    speed: float
    engine_temp: float


class AircraftEvent(BaseModel):
    event_id: str
    aircraft_id: str
    timestamp: str
    telemetry: Telemetry
    anomaly: Optional[str] = None

    def event_time(self) -> datetime:
        raw = self.timestamp.replace("Z", "+00:00")
        return datetime.fromisoformat(raw)


class CleanRecord(BaseModel):
    """写入 Iceberg 的清洗后行。"""

    event_id: str
    aircraft_id: str
    event_time: str
    ingest_time: str
    altitude: float
    speed: float
    engine_temp: float
    watermark: str
    late: bool = False


# ---------------------------------------------------------------------------
# 算子: Watermark / Dedup / Clean
# ---------------------------------------------------------------------------


@dataclass
class WatermarkAssigner:
    """
    模拟 Flink BoundedOutOfOrdernessWatermarks。

    watermark = max_event_time_seen - max_out_of_orderness
    - event_time < watermark                 → LATE
    - LATE 且落后超过 allowed_lateness       → DROP
    - 其余迟到数据仍允许进入窗口（纠正乱序）
    """

    max_out_of_orderness: timedelta
    allowed_lateness: timedelta
    max_event_time: Optional[datetime] = None
    watermark: Optional[datetime] = None
    late_accepted: int = 0
    late_dropped: int = 0

    def on_event(self, event_time: datetime) -> tuple[str, bool]:
        """
        返回 (decision, is_late)

        decision: ACCEPT | DROP
        """
        if self.max_event_time is None or event_time > self.max_event_time:
            self.max_event_time = event_time
            self.watermark = event_time - self.max_out_of_orderness

        assert self.watermark is not None
        if event_time >= self.watermark:
            return "ACCEPT", False

        lateness = self.watermark - event_time
        if lateness > self.allowed_lateness:
            self.late_dropped += 1
            return "DROP", True

        self.late_accepted += 1
        return "ACCEPT", True


@dataclass
class Deduplicator:
    """按 event_id 去重，使用有界 LRU，避免 Demo 长时间运行撑爆内存。"""

    max_size: int = DEDUP_CACHE_SIZE
    seen: OrderedDict[str, None] = field(default_factory=OrderedDict)
    dropped: int = 0

    def is_duplicate(self, event_id: str) -> bool:
        if event_id in self.seen:
            self.seen.move_to_end(event_id)
            self.dropped += 1
            return True
        self.seen[event_id] = None
        if len(self.seen) > self.max_size:
            self.seen.popitem(last=False)
        return False


@dataclass
class JobStats:
    ingested: int = 0
    parsed_fail: int = 0
    watermark_drop: int = 0
    watermark_late: int = 0
    dedup_drop: int = 0
    clean_drop: int = 0
    sunk: int = 0


# ---------------------------------------------------------------------------
# Source
# ---------------------------------------------------------------------------


def tail_jsonl(path: Path) -> Iterator[str]:
    """从文件当前位置跟随写入，模拟 Kafka 消费。文件不存在则等待创建。"""
    console.print(f"[src][FLINK-SOURCE] Waiting for JSONL: {path}[/]")
    while not path.exists():
        time.sleep(0.4)
    console.print(f"[src][FLINK-SOURCE] Tailing {path.resolve()}[/]")
    with path.open("r", encoding="utf-8") as handle:
        handle.seek(0, os.SEEK_END)
        while True:
            line = handle.readline()
            if not line:
                time.sleep(0.1)
                continue
            yield line


def kafka_lines(bootstrap: str, topic: str, group_id: str) -> Optional[Iterator[str]]:
    try:
        from kafka import KafkaConsumer  # type: ignore
    except ImportError:
        return None

    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap,
            group_id=group_id,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            value_deserializer=lambda v: v.decode("utf-8"),
            consumer_timeout_ms=1_000,
            request_timeout_ms=2_000,
            api_version=(2, 8, 0),
        )
        # 触发一次集群探测
        consumer.topics()
    except Exception as exc:  # noqa: BLE001
        console.print(f"[dim][FLINK-SOURCE] Kafka unavailable ({bootstrap}): {exc}[/]")
        return None

    console.print(f"[src][FLINK-SOURCE] Consuming Kafka topic={topic}  bootstrap={bootstrap}[/]")

    def _iter() -> Iterator[str]:
        try:
            while True:
                batch = consumer.poll(timeout_ms=400)
                if not batch:
                    continue
                for records in batch.values():
                    for record in records:
                        yield record.value
        finally:
            consumer.close()

    return _iter()


def open_source(args: argparse.Namespace) -> Iterator[str]:
    if args.source in ("auto", "kafka") and not args.no_kafka:
        stream = kafka_lines(args.bootstrap, args.topic, args.group)
        if stream is not None:
            return stream
        if args.source == "kafka":
            console.print("[clean][FLINK-SOURCE] --source kafka 但无法连接，退出[/]")
            raise SystemExit(2)
    return tail_jsonl(args.jsonl)


# ---------------------------------------------------------------------------
# Sink
# ---------------------------------------------------------------------------


class IcebergSink:
    """把清洗后的行追加到本地目录，路径刻意做成 Iceberg table layout。"""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("a", encoding="utf-8")
        self.written = 0

    def write(self, row: CleanRecord) -> None:
        self.handle.write(row.model_dump_json() + "\n")
        self.handle.flush()
        self.written += 1

    def close(self) -> None:
        self.handle.close()


# ---------------------------------------------------------------------------
# 控制台
# ---------------------------------------------------------------------------


def print_banner(args: argparse.Namespace, source_name: str) -> None:
    body = (
        "[bold white]飞机传感器实时数据 Lakehouse  ·  Flink Job (simulated)[/]\n"
        f"[dim]source[/]     {source_name}\n"
        f"[dim]watermark[/]  max-out-of-orderness = {args.ooo}s    "
        f"allowed-lateness = {args.lateness}s\n"
        f"[dim]dedup[/]      key = event_id     cache = {DEDUP_CACHE_SIZE}\n"
        f"[dim]clean[/]      drop engine_temp > {args.temp_limit:g}\n"
        f"[dim]sink[/]       iceberg://local/{ICEBERG_SINK.as_posix()}"
    )
    console.print(
        Panel(
            body,
            title="[ok]>> FLINK TELEMETRY PIPELINE[/]",
            subtitle="[dim]Ctrl+C 停止[/]",
            border_style="bright_green",
            box=box.DOUBLE,
            padding=(1, 2),
        )
    )
    console.print()


def print_stats(stats: JobStats, wm: WatermarkAssigner) -> None:
    table = Table(
        box=box.SIMPLE_HEAVY,
        header_style="bold cyan",
        title="[stat]── flink operator snapshot ──[/]",
        expand=False,
    )
    table.add_column("operator", style="dim")
    table.add_column("metric", style="white")
    table.add_column("value", justify="right", style="bold white")
    table.add_row("source", "ingested", str(stats.ingested))
    table.add_row("parse", "invalid json", str(stats.parsed_fail))
    table.add_row("watermark", "late accepted", str(wm.late_accepted))
    table.add_row("watermark", "late dropped", str(wm.late_dropped))
    table.add_row("dedup", "dropped", str(stats.dedup_drop))
    table.add_row("clean", "overheat dropped", str(stats.clean_drop))
    table.add_row("iceberg", "written", str(stats.sunk))
    if wm.watermark is not None:
        table.add_row("watermark", "current", _fmt(wm.watermark))
    console.print(table)
    console.print()


def _fmt(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%H:%M:%S.%f")[:-3] + "Z"


# ---------------------------------------------------------------------------
# 处理管线
# ---------------------------------------------------------------------------


def process_line(
    raw: str,
    wm: WatermarkAssigner,
    dedup: Deduplicator,
    sink: IcebergSink,
    stats: JobStats,
    temp_limit: float,
) -> None:
    stats.ingested += 1
    try:
        event = AircraftEvent.model_validate_json(raw)
    except (ValidationError, json.JSONDecodeError) as exc:
        stats.parsed_fail += 1
        console.print(f"[clean][FLINK-CLEAN] Dropped invalid payload: {exc}[/]")
        return

    event_time = event.event_time()
    decision, is_late = wm.on_event(event_time)
    wm_text = _fmt(wm.watermark) if wm.watermark else "-"

    if is_late:
        stats.watermark_late += 1
        if decision == "DROP":
            stats.watermark_drop += 1
            console.print(
                f"[wm][FLINK-WATERMARK] Dropped too-late event "
                f"event_id={event.event_id[:8]}  aircraft={event.aircraft_id}  "
                f"event_time={_fmt(event_time)}  watermark={wm_text}[/]"
            )
            return
        console.print(
            f"[wm][FLINK-WATERMARK] Reordered late event "
            f"event_id={event.event_id[:8]}  aircraft={event.aircraft_id}  "
            f"event_time={_fmt(event_time)}  watermark={wm_text}[/]"
        )

    if dedup.is_duplicate(event.event_id):
        stats.dedup_drop += 1
        console.print(
            f"[dedup][FLINK-DEDUP] Dropped duplicate event_id: {event.event_id}[/]"
        )
        return

    if event.telemetry.engine_temp > temp_limit:
        stats.clean_drop += 1
        console.print(
            f"[clean][FLINK-CLEAN] Dropped dirty record "
            f"event_id={event.event_id[:8]}  aircraft={event.aircraft_id}  "
            f"engine_temp={event.telemetry.engine_temp:.1f} > {temp_limit:g}[/]"
        )
        return

    row = CleanRecord(
        event_id=event.event_id,
        aircraft_id=event.aircraft_id,
        event_time=event.timestamp,
        ingest_time=datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z"),
        altitude=event.telemetry.altitude,
        speed=event.telemetry.speed,
        engine_temp=event.telemetry.engine_temp,
        watermark=wm_text,
        late=is_late,
    )
    sink.write(row)
    stats.sunk += 1
    console.print(
        f"[sink][FLINK-SINK] Written to Iceberg: "
        f"db.aircraft_telemetry  aircraft={event.aircraft_id}  "
        f"event_id={event.event_id}  "
        f"alt={event.telemetry.altitude:.1f}  "
        f"temp={event.telemetry.engine_temp:.1f}[/]"
    )


# ---------------------------------------------------------------------------
# 入口
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="模拟 Flink 实时处理作业")
    parser.add_argument("--source", choices=("auto", "kafka", "file"), default="auto")
    parser.add_argument(
        "--bootstrap",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument("--topic", default=KAFKA_TOPIC)
    parser.add_argument("--group", default="flink-telemetry-mvp")
    parser.add_argument("--jsonl", type=Path, default=DEFAULT_JSONL)
    parser.add_argument("--ooo", type=float, default=DEFAULT_OOO_SECONDS, help="乱序容忍秒数")
    parser.add_argument("--lateness", type=float, default=ALLOWED_LATENESS_SECONDS)
    parser.add_argument("--temp-limit", type=float, default=ENGINE_TEMP_LIMIT)
    parser.add_argument("--no-kafka", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    stats = JobStats()
    wm = WatermarkAssigner(
        max_out_of_orderness=timedelta(seconds=args.ooo),
        allowed_lateness=timedelta(seconds=args.lateness),
    )
    dedup = Deduplicator()
    sink = IcebergSink(ICEBERG_SINK)
    running = True

    def _stop(signum, _frame):  # noqa: ARG001
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, _stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _stop)

    source_name = "auto (Kafka → JSONL fallback)"
    print_banner(args, source_name)

    try:
        stream = open_source(args)
        for raw in stream:
            if not running:
                break
            raw = raw.strip()
            if not raw:
                continue
            process_line(raw, wm, dedup, sink, stats, args.temp_limit)
            if stats.ingested % 20 == 0:
                print_stats(stats, wm)
    except KeyboardInterrupt:
        running = False
    finally:
        sink.close()
        console.print()
        print_stats(stats, wm)
        console.print("[ok]flink job stopped[/]")
    return 0


if __name__ == "__main__":
    sys.exit(main())
