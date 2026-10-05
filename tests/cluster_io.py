"""Small helpers for the Flink REST API and Spark SQL checks."""

from __future__ import annotations

import json
import subprocess
import urllib.parse
import urllib.request


def _get(url: str) -> str:
    with urllib.request.urlopen(url, timeout=30) as response:
        return response.read().decode("utf-8")


def job_snapshot() -> dict:
    overview = json.loads(_get("http://localhost:8081/jobs/overview"))
    for job in overview.get("jobs", []):
        if job.get("name") == "lakehouse-aircraft-telemetry" and job.get("state") in {
            "RUNNING",
            "RESTARTING",
            "CREATED",
            "FAILING",
            "FAILED",
        }:
            return job
    raise RuntimeError(f"lakehouse job not found: {overview}")


def vertices(job_id: str) -> list[dict]:
    payload = json.loads(_get(f"http://localhost:8081/jobs/{job_id}"))
    return payload["vertices"]


def source_vertex_id(job_id: str) -> str:
    for vertex in vertices(job_id):
        if vertex["name"].startswith("Source: kafka_telemetry"):
            return vertex["id"]
    raise RuntimeError("Kafka source vertex not found")


def vertex_id_containing(job_id: str, text: str) -> str:
    for vertex in vertices(job_id):
        if text in vertex["name"]:
            return vertex["id"]
    raise RuntimeError(f"vertex containing {text!r} not found")


def metric_ids(job_id: str, vertex_id: str, subtask: int) -> list[str]:
    url = (
        f"http://localhost:8081/jobs/{job_id}/vertices/{vertex_id}"
        f"/subtasks/{subtask}/metrics"
    )
    payload = json.loads(_get(url))
    return [item["id"] for item in payload]


def metric(job_id: str, vertex_id: str, subtask: int, name: str) -> str:
    query = urllib.parse.urlencode({"get": name})
    url = (
        f"http://localhost:8081/jobs/{job_id}/vertices/{vertex_id}"
        f"/subtasks/{subtask}/metrics?{query}"
    )
    payload = json.loads(_get(url))
    if not payload:
        raise RuntimeError(f"metric {name} missing on subtask {subtask}")
    return payload[0]["value"]


SOURCE_RECORDS = "Source__kafka_telemetry[1].numRecordsIn"


def records_by_subtask(job_id: str, vertex_id: str, metric_name: str) -> list[int]:
    counts = []
    subtask = 0
    while subtask < 16:
        try:
            counts.append(int(float(metric(job_id, vertex_id, subtask, metric_name))))
        except RuntimeError:
            break
        subtask += 1
    return counts


def source_records(job_id: str) -> int:
    return sum(source_records_by_subtask(job_id))


def source_records_by_subtask(job_id: str) -> list[int]:
    return records_by_subtask(job_id, source_vertex_id(job_id), SOURCE_RECORDS)


def completed_checkpoints(job_id: str) -> int:
    payload = json.loads(_get(f"http://localhost:8081/jobs/{job_id}/checkpoints"))
    return int(payload["counts"]["completed"])


def backpressure(job_id: str, vertex_id: str) -> dict:
    return json.loads(
        _get(f"http://localhost:8081/jobs/{job_id}/vertices/{vertex_id}/backpressure")
    )


def spark_rows(sql: str) -> list[list[str]]:
    completed = subprocess.run(
        ["docker", "exec", "spark-iceberg", "spark-sql", "-e", sql],
        check=False,
        capture_output=True,
        text=True,
    )
    text = (completed.stdout or "") + "\n" + (completed.stderr or "")
    if completed.returncode != 0:
        raise RuntimeError(text[-2000:])
    rows: list[list[str]] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith(
            (
                "Setting ",
                "To adjust",
                "SLF4J",
                "WARNING",
                "Spark ",
                "26/",
                "25/",
                "INFO ",
                "WARN ",
                "ERROR ",
            )
        ):
            continue
        if "\t" not in stripped and " " in stripped:
            continue
        cells = [cell.strip() for cell in stripped.split("\t")]
        if cells and cells[0].lower() in {"event_id", "reason", "window_start", "cnt"}:
            continue
        rows.append(cells)
    return rows
