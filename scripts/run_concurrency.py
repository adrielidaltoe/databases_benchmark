"""Concurrency benchmark: the TPC-H throughput test on one engine at one scale
factor. See README section 9.

Per level (number of query streams S, default 1, 2, 4, 8):
  setup     fresh copies of orders and lineitem, built as in run_writes.py, so every
            level starts from the same data (Postgres schema c_sf<N>, autovacuum on as
            in production, the other 6 tables read from sf<N> via search_path;
            ClickHouse database c_sf<N>_rmt with all 8 tables as ReplacingMergeTree)
  cold      container stopped, its data evicted from the OS page cache, restarted
  run       S query streams + 1 corrections stream, all started together:
            - query stream i runs the 22 queries once, in the order of TPC-H stream i
              (spec Appendix A), each in its best version for that engine (fastest
              that completed in the single-user SF<N> run); ClickHouse with final = 1
            - the corrections stream applies S correction sets back to back, as TPC-H
              applies S refresh pairs: one run of every write-benchmark scenario
              except the restatement (a scheduled bulk job, and a Postgres timeout
              at SF 10), with each engine's production method (Postgres
              transactions, ClickHouse ReplacingMergeTree inserts)
Measured: each query's time and status, its slowdown against the single-user
warm median, queries per hour, the TPC-H throughput figure (S x 22 x 3600 / T x SF),
the corrections stream's times, and the container's CPU, peak memory and disk I/O
over the whole level (cgroup, as in run_queries.py). Timeouts (30 min) and memory
limits are results; a container that dies ends the level.

--stop-if-flat skips the remaining levels when, from 4 streams on, queries per hour
grows less than 5% over the previous level (the machine is saturated).

    python scripts/run_concurrency.py --engine clickhouse --sf 10
    python scripts/run_concurrency.py --engine postgres --sf 10 --stop-if-flat
    python scripts/run_concurrency.py --engine clickhouse --sf 1 --levels 1,2 --queries q01,q06
"""
import argparse
import csv
import json
import statistics
import subprocess
import threading
import time
from datetime import datetime, timezone

from check_rewrites import CH_CONN, TIMEOUT_S, make_runner
from common import CONTAINERS, RESULTS_DIR, ROOT, SCALE_FACTORS, TABLES, require_only
from run_queries import (QUERIES_DIR, ResourceMonitor, cgroup_dir, classify, cold_restart,
                         container_running, copy_as_replacing, latest, p95, settings_snapshot,
                         wait_ready)
from run_writes import SCENARIOS, ClickHouse, Postgres, runs_of, units

# TPC-H spec Appendix A: query order of each stream (stream 0 is the power test)
TPCH_STREAMS = {
    1: [21, 3, 18, 5, 11, 7, 6, 20, 17, 12, 16, 15, 13, 10, 2, 8, 14, 19, 9, 22, 1, 4],
    2: [6, 17, 14, 16, 19, 10, 9, 2, 15, 8, 5, 22, 12, 7, 13, 18, 1, 4, 20, 3, 11, 21],
    3: [8, 5, 4, 6, 17, 7, 1, 18, 22, 14, 9, 10, 15, 11, 20, 2, 21, 19, 13, 16, 12, 3],
    4: [5, 21, 14, 19, 15, 17, 12, 6, 4, 9, 8, 16, 11, 2, 10, 18, 1, 13, 7, 22, 3, 20],
    5: [21, 15, 4, 6, 7, 16, 19, 18, 14, 22, 11, 13, 3, 1, 2, 5, 8, 20, 12, 17, 10, 9],
    6: [10, 3, 15, 13, 6, 8, 9, 7, 4, 11, 22, 18, 12, 1, 5, 16, 2, 14, 19, 20, 17, 21],
    7: [18, 8, 20, 21, 2, 4, 22, 17, 1, 11, 9, 19, 3, 13, 5, 7, 10, 16, 6, 14, 15, 12],
    8: [19, 1, 15, 17, 5, 8, 9, 12, 14, 7, 4, 3, 20, 16, 6, 22, 10, 13, 2, 21, 18, 11],
}
assert all(sorted(order) == list(range(1, 23)) for order in TPCH_STREAMS.values())

CORRECTION_SCENARIOS = [s for s in SCENARIOS if s != "restatement"]
COPIED = ["orders", "lineitem"]  # the tables the corrections change
# engine -> label of the single-user results the best versions come from
BASELINE = {"postgres": "postgres", "clickhouse": "clickhouse_rmt"}
CSV_FIELDS = ["timestamp", "engine", "sf", "streams", "status", "wall_s", "queries", "ok",
              "failed", "queries_per_hour", "tpch_throughput", "slowdown_geomean",
              "slowdown_p95", "corrections_s", "corrections_done_s", "corrections_failed",
              "cpu_s", "avg_cores", "peak_mem_mb", "peak_anon_mb", "read_mb", "write_mb",
              "setup_s"]


def best_versions(engine: str, sf: int) -> tuple[str, dict]:
    """{query: (variant, sql, single-user warm median, cold)} from the latest single-user
    run: per query, the fastest version (warm median) that completed."""
    label = BASELINE[engine]
    try:
        data = latest(label, sf)
    except SystemExit:  # e.g. no ReplacingMergeTree run at this scale (smoke tests)
        label = engine
        data = latest(label, sf)
    best = {}
    for e in data["queries"]:
        if e["status"] != "ok":
            continue
        if e["query"] not in best or e["warm_median_s"] < best[e["query"]]["warm_s"]:
            best[e["query"]] = {"variant": e["variant"], "warm_s": e["warm_median_s"],
                                "cold_s": e.get("cold_s")}
    for q, b in best.items():
        sub = {"original": "", "rewritten": "rewritten/", "tuned": "tuned/"}[b["variant"]]
        b["sql"] = (QUERIES_DIR / engine / sub / f"{q}.sql").read_text()
    return f"queries_{label}_sf{sf}_{data['timestamp']}.json", best


def target(engine: str, sf: int) -> dict:
    """make_runner arguments for the level's copies."""
    if engine == "postgres":
        return {"database": f"c_sf{sf}, sf{sf}"}
    return {"database": f"c_sf{sf}_rmt", "ch_settings": {"final": 1}}


def check_no_lightweight_tables() -> None:
    """The write benchmark's lightweight-UPDATE copies hold ~1 GB of server memory while
    idle (README section 8): refuse to run with them attached."""
    import clickhouse_connect

    client = clickhouse_connect.get_client(**CH_CONN)
    n = int(client.command("SELECT count() FROM system.tables WHERE database LIKE 'w\\_sf%\\_lw'"))
    if n:
        raise SystemExit(f"{n} w_sf*_lw tables are attached and cost ~1 GB of server memory: "
                         "DETACH TABLE ... PERMANENTLY them first (README section 8)")


def setup(engine: str, sf: int):
    """Fresh copies for one level; returns the writer used by the corrections stream."""
    if engine == "postgres":
        eng = Postgres(sf, prefix="c")
        eng.setup()
        for t in COPIED:  # the write benchmark turns autovacuum off; production has it on
            eng.conn.execute(f"ALTER TABLE {t} SET (autovacuum_enabled = true)")
    else:
        eng = ClickHouse(sf, "replacing", prefix="c")
        eng.setup()
        copy_as_replacing(eng.client, sf, eng.db, [t for t in TABLES if t not in COPIED])
        eng.settle()
    return eng


def query_stream(engine, sf, stream, order, best, conn, t0, out, stop):
    run = make_runner(engine, sf, **conn)
    for seq, n in enumerate(order, 1):
        if stop.is_set():
            return
        q = f"q{n:02d}"
        b = best[q]
        start = time.perf_counter()
        entry = {"stream": stream, "seq": seq, "query": q, "variant": b["variant"],
                 "start_s": round(start - t0, 3), "status": "ok"}
        try:
            entry["rows"] = len(run(b["sql"]))
        except Exception as e:  # timeouts / memory limits are results, not crashes
            err = str(e).splitlines()[0][:300]
            entry["status"], entry["error"] = classify(err), err
            if not container_running(engine):
                entry["status"] = "container_died"
                stop.set()
        entry["s"] = round(time.perf_counter() - start, 3)
        if entry["status"] == "ok":
            entry["slowdown"] = round(entry["s"] / b["warm_s"], 3)
        out.append(entry)
        print(f"    s{stream} {seq:2d} {q} {entry['s']:9.2f}s  {entry['status']}", flush=True)


def corrections_stream(eng, sf, sets, t0, out, stop):
    for k, data_set in enumerate(sets, 1):
        for scenario, data in data_set:
            if stop.is_set():
                return
            start = time.perf_counter()
            entry = {"set": k, "scenario": scenario, "units": units(scenario, data),
                     "start_s": round(start - t0, 3), "status": "ok"}
            try:
                eng.apply(scenario, data)
            except Exception as e:
                err = str(e).splitlines()[0][:300]
                entry["status"], entry["error"] = classify(err), err
            entry["s"] = round(time.perf_counter() - start, 3)
            out.append(entry)


def summarize(level: dict, sf: int) -> dict:
    qs, wall = level["queries"], level["wall_s"]
    ok = [e for e in qs if e["status"] == "ok"]
    slow = [e["slowdown"] for e in ok]
    corr = level["corrections"]
    s = {"queries": len(qs), "ok": len(ok), "failed": len(qs) - len(ok),
         "queries_per_hour": round(len(ok) / wall * 3600, 1),
         "slowdown_geomean": round(statistics.geometric_mean(slow), 2) if slow else None,
         "slowdown_p95": round(p95(slow), 2) if slow else None,
         "corrections_s": round(sum(e["s"] for e in corr), 1),
         "corrections_done_s": round(max((e["start_s"] + e["s"] for e in corr), default=0), 1),
         "corrections_failed": sum(e["status"] != "ok" for e in corr),
         "failures": sorted({f"{e['query']} {e['status']}" for e in qs if e["status"] != "ok"})}
    # TPC-H's throughput figure is only defined when every query completed
    if s["failed"] == 0 and len(qs) == level["streams"] * 22:
        s["tpch_throughput"] = round(level["streams"] * 22 * 3600 / wall * sf, 1)
    return s


def run_level(engine, sf, streams, best, only, cgroup) -> dict:
    print(f"  {streams} stream(s): setup", flush=True)
    t_setup = time.perf_counter()
    eng = setup(engine, sf)
    setup_s = round(time.perf_counter() - t_setup, 1)
    runs = {sc: runs_of(sf, sc, streams) for sc in CORRECTION_SCENARIOS}  # before the clock
    sets = [[(sc, runs[sc][k][1]) for sc in CORRECTION_SCENARIOS] for k in range(streams)]
    cold_restart(engine, sf)
    eng.reconnect()
    conn = target(engine, sf)
    wait_ready(engine, sf, **conn)

    queries, corrections, stop = [], [], threading.Event()
    with ResourceMonitor(cgroup) as mon:
        t0 = time.perf_counter()
        threads = [threading.Thread(target=query_stream, args=(
            engine, sf, i, [n for n in TPCH_STREAMS[i] if not only or f"q{n:02d}" in only],
            best, conn, t0, queries, stop)) for i in range(1, streams + 1)]
        threads.append(threading.Thread(target=corrections_stream,
                                        args=(eng, sf, sets, t0, corrections, stop)))
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        wall = time.perf_counter() - t0
    res = mon.result
    res["avg_cores"] = round(res["cpu_s"] / wall, 2)
    level = {"streams": streams, "setup_s": setup_s, "wall_s": round(wall, 1),
             "status": "container_died" if stop.is_set() else "ok", "resources": res,
             "queries": sorted(queries, key=lambda e: (e["stream"], e["seq"])),
             "corrections": corrections}
    level["summary"] = summarize(level, sf)
    if stop.is_set():  # bring it back for the next level
        subprocess.run(["docker", "start", CONTAINERS[engine]], check=True, capture_output=True)
        wait_ready(engine, sf)
    s = level["summary"]
    print(f"  {streams} stream(s): {level['wall_s']:.0f}s  {s['queries_per_hour']} queries/h  "
          f"slowdown {s['slowdown_geomean']}x  failed {s['failed']}  "
          f"corrections {s['corrections_s']}s  cores {res['avg_cores']}  "
          f"peak anon {res['peak_anon_mb']:.0f} MB", flush=True)
    return level


def run_engine(engine: str, sf: int, levels: list[int], stop_if_flat: bool,
               only: set[str] | None) -> None:
    require_only(engine)
    wait_ready(engine, sf)
    if engine == "clickhouse":
        check_no_lightweight_tables()
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    source, best = best_versions(engine, sf)
    result = {"engine": engine, "sf": sf, "timestamp": ts, "timeout_s": TIMEOUT_S,
              "best_versions_from": source, "query_settings": target(engine, sf).get("ch_settings"),
              "best_versions": {q: {k: v for k, v in b.items() if k != "sql"}
                                for q, b in sorted(best.items())},
              "correction_scenarios": CORRECTION_SCENARIOS,
              **settings_snapshot(engine, sf), "levels": []}
    print(f"{engine} sf{sf}: best versions from {source}", flush=True)
    cgroup = cgroup_dir(engine)
    for streams in levels:
        prev = result["levels"][-1]["summary"]["queries_per_hour"] if result["levels"] else None
        result["levels"].append(run_level(engine, sf, streams, best, only, cgroup))
        save(result)  # after every level, so a crash keeps the finished ones
        qph = result["levels"][-1]["summary"]["queries_per_hour"]
        if stop_if_flat and streams >= 4 and prev and qph < prev * 1.05 and streams != levels[-1]:
            result["skipped_levels"] = levels[levels.index(streams) + 1:]
            print(f"  throughput flat ({prev} -> {qph} queries/h): skipping "
                  f"{result['skipped_levels']}", flush=True)
            break
    save(result, final=True)


def save(data: dict, final: bool = False) -> None:
    """Full run -> results/concurrency_<engine>_sf<N>_<ts>.json (rewritten after every
    level); one line per level -> results/concurrency.csv (at the end)."""
    RESULTS_DIR.mkdir(exist_ok=True)
    path = RESULTS_DIR / f"concurrency_{data['engine']}_sf{data['sf']}_{data['timestamp']}.json"
    path.write_text(json.dumps(data, indent=1))
    if not final:
        return
    summary = RESULTS_DIR / "concurrency.csv"
    new = not summary.exists()
    with summary.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        if new:
            w.writeheader()
        for lv in data["levels"]:
            w.writerow({k: data[k] for k in ("timestamp", "engine", "sf")} | lv | lv["resources"]
                       | lv["summary"])
    print(f"saved {path.relative_to(ROOT)}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", choices=list(BASELINE), required=True)
    ap.add_argument("--sf", type=int, choices=SCALE_FACTORS, required=True)
    ap.add_argument("--levels", default="1,2,4,8", help="query streams per level (max 8)")
    ap.add_argument("--stop-if-flat", action="store_true",
                    help="from 4 streams on, skip the remaining levels once queries/hour grows < 5%%")
    ap.add_argument("--queries", help="comma-separated subset (smoke test), e.g. q01,q06")
    args = ap.parse_args()
    levels = [int(x) for x in args.levels.split(",")]
    if not all(1 <= s <= len(TPCH_STREAMS) for s in levels):
        ap.error(f"levels must be between 1 and {len(TPCH_STREAMS)}")
    only = set(args.queries.split(",")) if args.queries else None
    run_engine(args.engine, args.sf, levels, args.stop_if_flat, only)


if __name__ == "__main__":
    main()
