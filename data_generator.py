"""
飞机传感器 Telemetry Producer
============================

模拟 3 架飞机持续产生传感器 JSON，并滚动打印到终端。

关键机制:
  - 每 0.5 秒产出一条事件（可配置）
  - 约 5% 注入异常: 乱序时间戳 (Out-of-order) 或重复 event_id
  - 少量脏数据: engine_temp > 1000，供下游 Flink 清洗演示
  - 同时写入本地 JSONL；若 Kafka 可用则一并投递
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

from pydantic import BaseModel, Field
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.theme import Theme
from rich.text import Text


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

AIRCRAFT_IDS = ("AC-101", "AC-102", "AC-103")
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
# 数据模型
# ---------------------------------------------------------------------------


class Telemetry(BaseModel):
    """单条传感器读数。"""

    altitude: float = Field(..., description="飞行高度，单位米")
    speed: float = Field(..., description="真空速，单位 km/h")
    engine_temp: float = Field(..., description="发动机排气温度，单位摄氏度")


class AircraftEvent(BaseModel):
    """Kafka / JSONL 上的标准事件信封。"""

    event_id: str
    aircraft_id: str
    timestamp: str
    telemetry: Telemetry
    anomaly: Optional[str] = Field(
        default=None,
        description="演示标记: late / duplicate / overheat，生产环境可省略",
    )


# ---------------------------------------------------------------------------
# 飞机飞行状态（随机游走，让曲线看起来像真实巡航）
# ---------------------------------------------------------------------------


@dataclass
class AircraftState:
    aircraft_id: str
    altitude: float
    speed: float
    engine_temp: float
    last_event_id: Optional[str] = None
    emitted: int = 0

    def tick(self) -> Telemetry:
        """让高度 / 速度 / 温度做小幅随机游走，并夹在合理区间内。"""
        self.altitude = _clamp(self.altitude + random.uniform(-80, 80), 8_800, 12_200)
        self.speed = _clamp(self.speed + random.uniform(-12, 12), 720, 920)
        self.engine_temp = _clamp(self.engine_temp + random.uniform(-8, 8), 520, 860)
        self.emitted += 1
        return Telemetry(
            altitude=round(self.altitude, 1),
            speed=round(self.speed, 1),
            engine_temp=round(self.engine_temp, 1),
        )


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _fleet() -> list[AircraftState]:
    """三架飞机的初始巡航剖面略有差异，方便录屏时一眼区分。"""
    return [
        AircraftState("AC-101", altitude=10_800, speed=860, engine_temp=690),
        AircraftState("AC-102", altitude=10_200, speed=820, engine_temp=640),
        AircraftState("AC-103", altitude=11_400, speed=880, engine_temp=710),
    ]


# ---------------------------------------------------------------------------
# Kafka（可选）
# ---------------------------------------------------------------------------


class OptionalKafka:
    """Kafka 可用则发送，不可用则静默降级，保证单机 Demo 仍能跑。"""

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
            console.print("[dim]kafka-python 未安装，仅本地 JSONL + 控制台输出[/]")
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
            # 一次 metadata 探测，避免“假连接”
            self.producer.partitions_for(self.topic)
            self.ok = True
        except Exception as exc:  # noqa: BLE001
            self.producer = None
            self.ok = False
            console.print(f"[dim]Kafka 不可用 ({self.bootstrap}): {exc}[/]")
            console.print("[dim]已降级为本地 JSONL，Flink 作业会自动 tail 该文件[/]")

    def send(self, event: AircraftEvent) -> None:
        if not self.ok or self.producer is None:
            return
        try:
            self.producer.send(
                self.topic,
                key=event.aircraft_id,
                value=event.model_dump_json(),
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
# 事件工厂
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
    aircraft: AircraftState,
    anomaly_rate: float,
    hot_rate: float,
    stats: GeneratorStats,
) -> AircraftEvent:
    """
    生成一条事件。约 anomaly_rate 的概率注入乱序或重复，
    另有 hot_rate 的概率把发动机温度拉到脏数据区间。
    """
    now = datetime.now(timezone.utc)
    telemetry = aircraft.tick()
    event_id = str(uuid.uuid4())
    anomaly: Optional[str] = None

    roll = random.random()
    if roll < anomaly_rate:
        # 一半乱序、一半重复（重复需要已有历史 event_id）
        if aircraft.last_event_id and random.random() < 0.5:
            event_id = aircraft.last_event_id
            anomaly = "duplicate"
            stats.duplicate += 1
        else:
            # 回拨 8~25 秒，超过 Flink 默认 5s watermark，必被识别为乱序
            delay = random.uniform(8, 25)
            now = now - timedelta(seconds=delay)
            anomaly = "late"
            stats.late += 1
    else:
        stats.normal += 1

    if random.random() < hot_rate:
        telemetry.engine_temp = round(random.uniform(1_050, 1_280), 1)
        anomaly = "overheat" if anomaly is None else f"{anomaly}+overheat"
        stats.overheat += 1

    if anomaly != "duplicate":
        aircraft.last_event_id = event_id

    stats.total += 1
    stats.per_ac[aircraft.aircraft_id] += 1

    return AircraftEvent(
        event_id=event_id,
        aircraft_id=aircraft.aircraft_id,
        timestamp=now.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        telemetry=telemetry,
        anomaly=anomaly,
    )


# ---------------------------------------------------------------------------
# 漂亮的控制台输出（方便 OBS 录屏）
# ---------------------------------------------------------------------------


def print_banner(interval: float, kafka: OptionalKafka) -> None:
    sink = "Kafka + JSONL" if kafka.ok else "本地 JSONL（Kafka 离线）"
    body = Text.from_markup(
        "[banner]飞机传感器实时数据 Lakehouse  ·  Producer[/]\n"
        f"[dim]fleet[/]  AC-101   AC-102   AC-103\n"
        f"[dim]rate[/]   每 [stat]{interval:.1f}s[/] 一条   "
        f"[dim]anomaly[/]  5% 乱序/重复   [dim]dirty[/]  3% 超温\n"
        f"[dim]sink[/]   {sink}   [dim]topic[/]  {KAFKA_TOPIC}"
    )
    console.print(
        Panel(
            body,
            title="[ok]>> TELEMETRY PRODUCER[/]",
            subtitle="[dim]Ctrl+C 停止[/]",
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
    if "late" in anomaly:
        return Text("  LATE  ", style="black on yellow")
    if "overheat" in anomaly:
        return Text("  HOT   ", style="white on bright_red")
    return Text(f" {anomaly.upper()} ", style="white on red")


def print_event(event: AircraftEvent) -> None:
    t = event.telemetry
    ac_style = AIRCRAFT_STYLE.get(event.aircraft_id, "white")
    wall = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    event_ts = event.timestamp[11:23] if len(event.timestamp) >= 23 else event.timestamp

    line = Text()
    line.append(f" {wall} ", style="dim")
    line.append(f" {event.aircraft_id} ", style=f"black on {ac_style.split()[-1]}")
    line.append("  ")
    line.append_text(_badge(event.anomaly))
    line.append("  ")
    line.append(f"alt={t.altitude:>8,.1f} m", style="bright_white")
    line.append("   ")
    line.append(f"spd={t.speed:>6.1f} km/h", style="bright_white")
    line.append("   ")
    temp_style = "hot" if t.engine_temp > 1000 else "bright_white"
    line.append(f"temp={t.engine_temp:>7.1f} °C", style=temp_style)
    line.append("   ")
    line.append(f"id={event.event_id[:8]}", style="dim")
    if event.anomaly == "late":
        line.append(f"   event_time={event_ts}", style="late")
    console.print(line)

    # 紧凑 JSON，OBS 里能看清完整 payload
    payload = event.model_dump(exclude_none=True)
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
# 主循环
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="飞机传感器 Telemetry Producer")
    parser.add_argument("--interval", type=float, default=DEFAULT_INTERVAL, help="发送间隔（秒）")
    parser.add_argument("--anomaly-rate", type=float, default=DEFAULT_ANOMALY_RATE)
    parser.add_argument("--hot-rate", type=float, default=DEFAULT_HOT_RATE)
    parser.add_argument(
        "--bootstrap",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap servers",
    )
    parser.add_argument("--no-kafka", action="store_true", help="强制不连接 Kafka")
    parser.add_argument("--jsonl", type=Path, default=LOCAL_SINK, help="本地 JSONL 路径")
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
                event = build_event(aircraft, args.anomaly_rate, args.hot_rate, stats)
                line = event.model_dump_json()
                sink.write(line + "\n")
                sink.flush()
                kafka.send(event)
                print_event(event)

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
