"""Shared paths, table list and helpers for the TPC-H generate/load scripts."""
import csv
import json
import re
import subprocess
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
SCHEMA_DIR = ROOT / "schema"
RESULTS_DIR = ROOT / "results"

SCALE_FACTORS = [1, 3, 10]
TABLES = ["region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"]

# Rows per batch sent to either engine. Same value for both so the client-side
# work (Parquet decode + network round-trips) is comparable.
BATCH_ROWS = 500_000

CONTAINERS = {"postgres": "bench_pg", "clickhouse": "bench_ch"}


def parquet_path(sf: int, table: str) -> Path:
    return DATA_DIR / f"sf{sf}" / f"{table}.parquet"


def require_only(engine: str) -> None:
    """Abort unless this engine's container is up and the other one is down."""
    out = subprocess.run(
        ["docker", "ps", "--format", "{{.Names}}"], capture_output=True, text=True, check=True
    ).stdout.split()
    mine = CONTAINERS[engine]
    other = next(c for e, c in CONTAINERS.items() if e != engine)
    if other in out:
        raise SystemExit(f"{other} is running - stop it first (never run both engines at once)")
    if mine not in out:
        profile = "pg" if engine == "postgres" else "ch"
        raise SystemExit(f"{mine} is not running - docker compose --profile {profile} up -d")


def read_sql(name: str) -> list[str]:
    """Split a schema/*.sql file into statements (strips -- comments)."""
    text = re.sub(r"--[^\n]*", "", (SCHEMA_DIR / name).read_text())
    return [s.strip() for s in text.split(";") if s.strip()]


class Stopwatch:
    """Accumulates wall-clock seconds per named phase."""

    def __init__(self):
        self.phases: dict[str, float] = {}

    @contextmanager
    def phase(self, name: str):
        t0 = time.perf_counter()
        try:
            yield
        finally:
            self.phases[name] = self.phases.get(name, 0.0) + time.perf_counter() - t0

    @property
    def total(self) -> float:
        return sum(self.phases.values())


def save_result(engine: str, sf: int, result: dict, summary: dict) -> Path:
    """Write the full run to results/load_<engine>_sf<N>_<ts>.json and append a
    one-line summary to results/loads.csv."""
    RESULTS_DIR.mkdir(exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = RESULTS_DIR / f"load_{engine}_sf{sf}_{ts}.json"
    path.write_text(json.dumps({"engine": engine, "sf": sf, "timestamp": ts, **result}, indent=2, default=str))

    summary_path = RESULTS_DIR / "loads.csv"
    row = {"timestamp": ts, "engine": engine, "sf": sf, **summary}
    new = not summary_path.exists()
    with summary_path.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(row))
        if new:
            w.writeheader()
        w.writerow(row)
    return path


def fmt_bytes(n: float) -> str:
    for unit in ["B", "KB", "MB", "GB"]:
        if n < 1024:
            return f"{n:,.1f} {unit}"
        n /= 1024
    return f"{n:,.1f} TB"
