"""Timed TPC-H query benchmark: the 22 standard queries plus the 6 CTE rewrites
(and, on ClickHouse, the per-query tuned versions), on one engine at one scale factor.

Variants: original (as written), rewritten (queries/<engine>/rewritten, CTE
decorrelation, same SQL on both engines), tuned (queries/<engine>/tuned, only
ClickHouse Q3/Q5/Q8/Q9: the original SQL plus a per-query join_algorithm so they
fit the memory cap at SF10 - see the README section on ClickHouse join tuning).

Per query (each variant):
  1. cold run  - container stopped, the engine's data files evicted from the OS
                 page cache, container started, then the query runs once
                 (engine buffers + OS cache of the data both empty)
  2. warm runs - --runs repetitions right after; median, p95, min reported
A timeout (30 min) or error (e.g. memory limit) is recorded as the result and
the remaining runs of that query are skipped.

Resource use per run comes from the container's cgroup v2 files on the host
(same source for both engines, no root needed):
  cpu_s        CPU time used by the container (user + system) during the query
  avg_cores    cpu_s / wall time (3.0 = all three allowed CPUs busy)
  peak_mem_mb  peak memory.current: everything charged to the container, incl.
               page cache and Postgres shared_buffers (shared memory)
  peak_anon_mb peak anonymous memory: the query's working memory (hash tables,
               sort and aggregation buffers) - both peaks sampled every 100 ms
  read_mb / write_mb  bytes read from / written to the block device (io.stat);
               near 0 on warm runs, the real disk I/O on cold runs

Times are client-side wall clock including fetching the result (small for all
TPC-H queries), measured the same way on both engines. ClickHouse query cache
is off; Postgres has no result cache.

Eviction needs no root: a throwaway, unprivileged container mounts the engine's
data volume read-only and calls posix_fadvise(DONTNEED) on every file
(`dd iflag=nocache count=0`). The engine is stopped first, so all its pages are
clean and evictable. Unlike drop_caches this leaves the OS's other cached files
(binaries, libraries) alone - only the database's own data is cold.
--no-cold skips the cold runs (and the restarts).

Scan/output ratio, measured on the first run of each query (cold run, or the
warm-up with --no-cold) - the counts are deterministic, so once is enough:
  scan_rows    rows read from tables (+ index entries on Postgres)
  scan_mb      bytes read by the query, as each engine counts them:
               ClickHouse read_bytes from system.query_log (uncompressed bytes
               of the columns read); Postgres buffer accesses x 8 KB from
               pg_statio_* (whole row pages, counted again on every re-visit,
               e.g. by index nested loops)
  scan_ratio   scan_rows / result rows
Postgres counts are deltas of the cumulative pg_stat_* counters around the
query (pg_stat_force_next_flush() makes the backend publish them right away).

    python scripts/run_queries.py --engine postgres --sf 1
    python scripts/run_queries.py --engine clickhouse --sf 1 --runs 5 --queries q01,q17
    python scripts/run_queries.py --compare --sf 1     # cross-engine result check
"""
import argparse
import csv
import json
import logging
import statistics
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from check_rewrites import CH_CONN, PG_DSN, REWRITTEN, TIMEOUT_S, make_runner, normalize, same_rows
from common import CONTAINERS, RESULTS_DIR, ROOT, SCALE_FACTORS, require_only

# clickhouse-connect logs every refused connection while the server restarts;
# the exceptions still reach wait_ready / timed, so the log lines are just noise
logging.getLogger("clickhouse_connect").setLevel(logging.CRITICAL)

QUERIES_DIR = ROOT / "queries"
ALL_QUERIES = [f"q{i:02d}" for i in range(1, 23)]


def query_list(engine: str, only: set[str] | None) -> list[tuple[str, str, str]]:
    """(query, variant, sql) for every original, rewrite and tuned version, in query order."""
    out = []
    for q in ALL_QUERIES:
        if only and q not in only:
            continue
        out.append((q, "original", (QUERIES_DIR / engine / f"{q}.sql").read_text()))
        if q in REWRITTEN:
            out.append((q, "rewritten", (QUERIES_DIR / engine / "rewritten" / f"{q}.sql").read_text()))
        tuned = QUERIES_DIR / engine / "tuned" / f"{q}.sql"
        if tuned.exists():
            out.append((q, "tuned", tuned.read_text()))
    return out


EVICT_SH = ("find /d -type f -exec sh -c "
            "'for f; do dd if=\"$f\" iflag=nocache count=0 status=none; done' sh {} +")


def evict_data_files(engine: str) -> float:
    """Evict the (stopped) engine's data volume from the OS page cache; returns seconds taken."""
    container = CONTAINERS[engine]
    fmt = '{{.Config.Image}} {{range .Mounts}}{{if eq .Type "volume"}}{{.Name}}{{end}}{{end}}'
    image, volume = subprocess.run(["docker", "inspect", "-f", fmt, container],
                                   capture_output=True, text=True, check=True).stdout.split()
    t0 = time.perf_counter()
    subprocess.run(["sync"], check=True)  # flush any dirty pages so all are evictable
    subprocess.run(["docker", "run", "--rm", "--network", "none", "-v", f"{volume}:/d:ro",
                    "--entrypoint", "sh", image, "-c", EVICT_SH], check=True, capture_output=True)
    return time.perf_counter() - t0


def wait_ready(engine: str, sf: int, timeout: float = 120):
    """Return a fresh run(sql) once the engine answers queries."""
    t0 = time.perf_counter()
    while True:
        try:
            run = make_runner(engine, sf)
            run("SELECT 1")
            return run
        except Exception:
            if time.perf_counter() - t0 > timeout:
                raise
            time.sleep(1)


def cold_restart(engine: str, sf: int):
    """Stop the container, evict its data files from the OS page cache, start it again."""
    container = CONTAINERS[engine]
    subprocess.run(["docker", "stop", container], check=True, capture_output=True)
    evict_data_files(engine)
    subprocess.run(["docker", "start", container], check=True, capture_output=True)
    return wait_ready(engine, sf)


def container_running(engine: str) -> bool:
    out = subprocess.run(["docker", "ps", "--format", "{{.Names}}"],
                         capture_output=True, text=True).stdout.split()
    return CONTAINERS[engine] in out


def cgroup_dir(engine: str) -> Path:
    cid = subprocess.run(["docker", "inspect", "-f", "{{.Id}}", CONTAINERS[engine]],
                         capture_output=True, text=True, check=True).stdout.strip()
    return Path(f"/sys/fs/cgroup/system.slice/docker-{cid}.scope")


class ResourceMonitor:
    """Container CPU, memory and disk I/O over one query, from cgroup v2 files."""

    INTERVAL_S = 0.1

    def __init__(self, cgroup: Path):
        self.cg = cgroup

    def _read_kv(self, name: str) -> dict[str, int]:
        return {k: int(v) for k, v in (line.split() for line in (self.cg / name).read_text().splitlines())}

    def _io(self) -> tuple[int, int]:
        rbytes = wbytes = 0
        for line in (self.cg / "io.stat").read_text().splitlines():
            fields = dict(f.split("=") for f in line.split()[1:])
            rbytes += int(fields.get("rbytes", 0))
            wbytes += int(fields.get("wbytes", 0))
        return rbytes, wbytes

    def _sample(self):
        while not self._stop.wait(self.INTERVAL_S):
            self._mem = max(self._mem, int((self.cg / "memory.current").read_text()))
            self._anon = max(self._anon, self._read_kv("memory.stat")["anon"])

    def __enter__(self):
        self._cpu0 = self._read_kv("cpu.stat")["usage_usec"]
        self._io0 = self._io()
        self._mem = int((self.cg / "memory.current").read_text())
        self._anon = self._read_kv("memory.stat")["anon"]
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._sample, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._thread.join()
        try:
            cpu_s = (self._read_kv("cpu.stat")["usage_usec"] - self._cpu0) / 1e6
            r, w = self._io()
        except FileNotFoundError:  # container died mid-query
            cpu_s, r, w = float("nan"), *self._io0
        self.result = {
            "cpu_s": round(cpu_s, 3),
            "peak_mem_mb": round(self._mem / 2**20, 1),
            "peak_anon_mb": round(self._anon / 2**20, 1),
            "read_mb": round((r - self._io0[0]) / 2**20, 1),
            "write_mb": round((w - self._io0[1]) / 2**20, 1),
        }
        return False


def timed(run, sql: str, cgroup: Path, settings: dict | None = None
          ) -> tuple[float, list | None, str | None, dict]:
    """Run once; return (seconds, rows, error, resource usage)."""
    with ResourceMonitor(cgroup) as mon:
        t0 = time.perf_counter()
        try:
            rows, err = run(sql, settings), None
        except Exception as e:  # timeouts / memory limits are results, not crashes
            rows, err = None, str(e).splitlines()[0][:300]
        secs = time.perf_counter() - t0
    res = mon.result
    res["avg_cores"] = round(res["cpu_s"] / secs, 2) if secs else None
    return secs, rows, err, res


PG_SCAN_SQL = """
SELECT
    (SELECT coalesce(sum(seq_tup_read), 0) FROM pg_stat_user_tables WHERE schemaname = %(s)s)
    + (SELECT coalesce(sum(idx_tup_read), 0) FROM pg_stat_user_indexes WHERE schemaname = %(s)s),
    (SELECT coalesce(sum(heap_blks_read + heap_blks_hit
                         + coalesce(idx_blks_read, 0) + coalesce(idx_blks_hit, 0)), 0)
     FROM pg_statio_user_tables WHERE schemaname = %(s)s)
"""


def pg_scan_counters(conn, sf: int) -> tuple[int, int]:
    """Cumulative (rows read, buffer pages accessed) over the sf<N> tables."""
    conn.execute("SELECT pg_stat_force_next_flush()")  # publish this backend's pending counts
    rows, pages = conn.execute(PG_SCAN_SQL, {"s": f"sf{sf}"}).fetchone()
    return int(rows), int(pages)


def timed_with_scan(engine: str, sf: int, run, sql: str, cgroup: Path):
    """timed() plus the rows/bytes the query read. The extra bookkeeping queries
    run outside the timed window."""
    if engine == "postgres":
        before = pg_scan_counters(run.handle, sf)
        secs, rows, err, res = timed(run, sql, cgroup)
        after = pg_scan_counters(run.handle, sf)
        scan_rows, scan_bytes = after[0] - before[0], (after[1] - before[1]) * 8192
    else:
        tag = f"bench-{time.time_ns()}"
        secs, rows, err, res = timed(run, sql, cgroup, settings={"log_comment": tag})
        try:
            client = run.handle
            client.command("SYSTEM FLUSH LOGS")
            hit = client.query(
                "SELECT read_rows, read_bytes FROM system.query_log WHERE log_comment = {t:String} "
                "AND type IN ('QueryFinish', 'ExceptionWhileProcessing') LIMIT 1",
                parameters={"t": tag}).result_rows
        except Exception:  # server gone (e.g. OOM-killed): no scan stats for this run
            hit = []
        scan_rows, scan_bytes = hit[0] if hit else (None, None)
    scan = None
    if scan_rows is not None:
        scan = {"scan_rows": scan_rows, "scan_mb": round(scan_bytes / 2**20, 1)}
        if rows is not None:
            scan["scan_ratio"] = round(scan_rows / max(len(rows), 1), 1)
    return secs, rows, err, res, scan


RESOURCE_KEYS = ["cpu_s", "avg_cores", "peak_mem_mb", "peak_anon_mb", "read_mb", "write_mb"]


def classify(err: str) -> str:
    e = err.lower()
    if "timeout" in e or "timeout_exceeded" in e or "canceling statement" in e:
        return "timeout"
    if "memory" in e:
        return "memory_limit"
    return "error"


def p95(xs: list[float]) -> float:
    if len(xs) == 1:
        return xs[0]
    return statistics.quantiles(xs, n=20, method="inclusive")[18]


def settings_snapshot(engine: str, sf: int) -> dict:
    """Engine version + every non-default setting, saved with the results."""
    if engine == "postgres":
        import psycopg

        with psycopg.connect(PG_DSN) as conn:
            return {
                "version": conn.execute("SELECT version()").fetchone()[0],
                "settings": dict(conn.execute(
                    "SELECT name, setting || coalesce(unit, '') FROM pg_settings "
                    "WHERE source NOT IN ('default', 'override') ORDER BY name").fetchall()),
            }
    import clickhouse_connect

    client = clickhouse_connect.get_client(**CH_CONN, database=f"sf{sf}")
    return {
        "version": client.query("SELECT version()").result_rows[0][0],
        "settings": dict(client.query(
            "SELECT name, value FROM system.settings WHERE changed ORDER BY name").result_rows),
    }


def run_engine(engine: str, sf: int, runs: int, cold: bool, only: set[str] | None) -> None:
    require_only(engine)
    started = datetime.now(timezone.utc)
    ts = started.strftime("%Y%m%dT%H%M%SZ")
    run = wait_ready(engine, sf)
    settings = settings_snapshot(engine, sf)
    cgroup = cgroup_dir(engine)  # docker stop/start keeps the container id
    results = []

    for q, variant, sql in query_list(engine, only):
        entry = {"query": q, "variant": variant, "status": "ok"}
        if cold:
            run = cold_restart(engine, sf)
            secs, rows, err, res, scan = timed_with_scan(engine, sf, run, sql, cgroup)
            entry["cold_s"] = round(secs, 3)
            entry["cold_resources"] = res
        else:  # untimed warm-up, also the result check
            secs, rows, err, res, scan = timed_with_scan(engine, sf, run, sql, cgroup)
        if scan:
            entry.update(scan)
        warm, warm_res = [], []
        if err is None:
            entry["rows"] = len(rows)
            entry["result"] = [[normalize(v) for v in r] for r in rows]
            for _ in range(runs):
                secs, _, err, res = timed(run, sql, cgroup)
                if err:
                    break
                warm.append(secs)
                warm_res.append(res)
        if err:
            entry["status"] = classify(err)
            entry["error"] = err
            entry["failed_after_s"] = round(secs, 3)
            entry["failed_resources"] = res  # e.g. how much memory it reached before the limit
            if not container_running(engine):  # e.g. OOM-killed: bring it back for the next query
                entry["status"] = "container_died"
                subprocess.run(["docker", "start", CONTAINERS[engine]], check=True, capture_output=True)
            run = wait_ready(engine, sf)
        if warm:
            entry["warm_s"] = [round(x, 3) for x in warm]
            entry["warm_median_s"] = round(statistics.median(warm), 3)
            entry["warm_p95_s"] = round(p95(warm), 3)
            entry["warm_min_s"] = round(min(warm), 3)
            entry["warm_resources"] = warm_res
            # per-metric median over the warm runs, for the CSV summary
            entry.update({f"warm_{k}": round(statistics.median(r[k] for r in warm_res), 3)
                          for k in RESOURCE_KEYS})
        results.append(entry)
        cold_txt = f"cold {entry['cold_s']:8.2f}s  " if "cold_s" in entry else ""
        warm_txt = (f"warm median {entry['warm_median_s']:8.2f}s  p95 {entry['warm_p95_s']:8.2f}s  "
                    f"cores {entry['warm_avg_cores']:4.2f}  anon {entry['warm_peak_anon_mb']:7.1f} MB"
                    if warm else f"{entry['status'].upper()} after {entry.get('failed_after_s', 0):.1f}s")
        scan_txt = f"  scan {entry['scan_rows']:>11,} rows" if "scan_rows" in entry else ""
        print(f"  {q} {variant:<9}  {cold_txt}{warm_txt}{scan_txt}", flush=True)

    cold_method = ("engine stopped; its data volume evicted from the OS page cache with "
                   "posix_fadvise(DONTNEED) per file; engine restarted") if cold else None
    save(engine, sf, ts, {"engine": engine, "sf": sf, "timestamp": ts, "runs": runs, "cold": cold,
                          "cold_method": cold_method,
                          "timeout_s": TIMEOUT_S, **settings, "queries": results})


def save(engine: str, sf: int, ts: str, data: dict) -> None:
    """Full run -> results/queries_<engine>_sf<N>_<ts>.json; one line per query -> results/queries.csv."""
    RESULTS_DIR.mkdir(exist_ok=True)
    path = RESULTS_DIR / f"queries_{engine}_sf{sf}_{ts}.json"
    path.write_text(json.dumps(data, indent=1))

    fields = ["timestamp", "engine", "sf", "query", "variant", "status", "rows",
              "scan_rows", "scan_mb", "scan_ratio", "cold_s",
              "warm_median_s", "warm_p95_s", "warm_min_s", "runs",
              *(f"warm_{k}" for k in RESOURCE_KEYS), *(f"cold_{k}" for k in RESOURCE_KEYS)]
    summary_path = RESULTS_DIR / "queries.csv"
    new = not summary_path.exists()
    with summary_path.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        if new:
            w.writeheader()
        for e in data["queries"]:
            cold_res = {f"cold_{k}": v for k, v in e.get("cold_resources", {}).items()}
            w.writerow({"timestamp": ts, "engine": engine, "sf": sf, "runs": data["runs"], **e, **cold_res})
    print(f"saved {path.relative_to(ROOT)}")


def latest(engine: str, sf: int) -> dict:
    paths = sorted(RESULTS_DIR.glob(f"queries_{engine}_sf{sf}_*.json"))
    if not paths:
        raise SystemExit(f"no results/queries_{engine}_sf{sf}_*.json yet")
    return json.loads(paths[-1].read_text())


def compare_engines(sf: int) -> None:
    """Check the latest Postgres and ClickHouse runs returned the same results.
    ClickHouse tuned versions are checked against the Postgres original."""
    pg, ch = ({(e["query"], e["variant"]): e for e in latest(eng, sf)["queries"]}
              for eng in ("postgres", "clickhouse"))
    for (q, v) in [k for k in ch if k[1] == "tuned" and (k[0], "original") in pg]:
        pg[(q, v)] = pg[(q, "original")]
    for key in sorted(pg.keys() & ch.keys()):
        a, b = pg[key].get("result"), ch[key].get("result")
        label = f"  {key[0]} {key[1]:<9}"
        if a is None or b is None:
            print(f"{label}  skipped (pg {pg[key]['status']}, ch {ch[key]['status']})")
            continue
        diff = same_rows(a, b)
        if diff is None:
            print(f"{label}  MATCH")
        elif same_rows(sorted(a, key=repr), sorted(b, key=repr)) is None:
            # ties / string collation (Postgres sorts by locale, ClickHouse by bytes)
            print(f"{label}  MATCH (row order differs: {diff})")
        else:
            print(f"{label}  DIFF ({diff})")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sf", type=int, choices=SCALE_FACTORS, required=True)
    ap.add_argument("--engine", choices=["postgres", "clickhouse"])
    ap.add_argument("--runs", type=int, default=5, help="warm runs per query (default 5)")
    ap.add_argument("--no-cold", action="store_true", help="skip cold runs and restarts")
    ap.add_argument("--queries", help="comma-separated subset, e.g. q01,q17")
    ap.add_argument("--compare", action="store_true", help="compare latest Postgres vs ClickHouse results")
    args = ap.parse_args()
    if args.compare:
        compare_engines(args.sf)
    elif args.engine:
        only = set(args.queries.split(",")) if args.queries else None
        run_engine(args.engine, args.sf, args.runs, not args.no_cold, only)
    else:
        ap.error("give --engine or --compare")


if __name__ == "__main__":
    main()
