# 飞机传感器实时数据 Lakehouse（MVP）

用最小可运行代码演示一条真实的实时数仓链路：

```
Python Producer  →  Kafka / 本地 JSONL  →  Flink 模拟作业  →  Iceberg 目录
     3 架飞机           乱序 / 重复 / 超温           Watermark · Dedup · Clean
```

本仓库是 **可录屏的本地 Demo**：不依赖 Flink / Spark 集群也能跑通核心语义。Kafka 只是加分项，没起来时自动降级到文件总线。

## 项目结构

```
├── data_generator.py      # 飞机 Telemetry Producer（彩色滚动日志）
├── flink_job.py           # Watermark / 去重 / 清洗 + Iceberg sink 模拟
├── requirements.txt
└── README.md
```

运行后会自动生成（已在 `.gitignore` 中）：

```
data/telemetry.jsonl
warehouse/iceberg/db/aircraft_telemetry/data.jsonl
```

## 事件 Schema

```json
{
  "event_id": "7c2e0d2a-...",
  "aircraft_id": "AC-101",
  "timestamp": "2026-09-25T15:01:03.204Z",
  "telemetry": {
    "altitude": 10821.4,
    "speed": 854.2,
    "engine_temp": 688.1
  }
}
```

Producer 会以约 **5%** 概率注入：

| 异常 | 含义 | 下游表现 |
| --- | --- | --- |
| `late` | 时间戳回拨 8~25 秒 | `[FLINK-WATERMARK]` 识别乱序并纠正 / 丢弃过晚事件 |
| `duplicate` | 复用上一条 `event_id` | `[FLINK-DEDUP] Dropped duplicate event_id` |
| `overheat` | `engine_temp > 1000` | `[FLINK-CLEAN]` 拦截脏数据 |

## 快速开始

```powershell
python -m pip install -r requirements.txt
```

开两个终端：

```powershell
python data_generator.py
python flink_job.py
```

`data_generator.py` 每 0.5 秒打印一条带颜色的传感器日志；`flink_job.py` 会 tail 同一份 `data/telemetry.jsonl`，并打印：

```
[FLINK-WATERMARK] Reordered late event ...
[FLINK-DEDUP] Dropped duplicate event_id: ...
[FLINK-CLEAN] Dropped dirty record engine_temp=1124.0 > 1000
[FLINK-SINK] Written to Iceberg: db.aircraft_telemetry ...
```

## 基础设施（可选）

Kafka / Flink / Iceberg 使用本机共享的 `E:\project2608\platform-infra`，本项目不再自带 compose：

```powershell
cd E:\project2608\platform-infra
.\infra.ps1 up lakehouse-demo      # messaging + stream + lakehouse
```

两个脚本默认连接 `localhost:9092`，topic 为 `aircraft-telemetry`。Kafka 不通时自动走本地 JSONL，无需改代码。

```powershell
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
python data_generator.py
python flink_job.py --source kafka
```

## 常用参数

```powershell
python data_generator.py --interval 0.5 --anomaly-rate 0.05 --hot-rate 0.03
python flink_job.py --ooo 5 --lateness 15 --temp-limit 1000
```

## 处理语义（对应真实 Flink）

1. **Watermark**：`watermark = max(event_time) - 5s`。迟到但未超过 15s allowed lateness 的事件会被重排后放行；更晚的直接丢弃。
2. **Deduplication**：按 `event_id` LRU 去重（缓存 2000 条）。
3. **Clean**：`engine_temp > 1000` 视为传感器脏数据，不入湖。
4. **Sink**：追加写入本地 Iceberg-style 目录，便于后续接 Spark / 真 Iceberg catalog。
