"""ClickHouse join-algorithm tests at SF10: why Q3/Q5/Q8/Q9 get a per-query setting
(queries/clickhouse/tuned/) instead of a global one in clickhouse/users.d/profile.xml.

With the default (in-memory hash) join, Q3, Q5, Q8, Q9 and the original Q21 hit
the memory limit at SF10. Two steps:

  failing  every candidate config on those five queries (best of 2 runs; a
           failure is recorded and the config's remaining run skipped)
  sweep    one config on every query and version (1 warm-up checked against
           Postgres, then 3 runs, median) vs the default's warm median from the
           latest results/queries_clickhouse_sf10_*.json - what the config would
           cost the queries that already work if it were set in the profile

Settings are passed per query, so the server profile is not touched. Results go
to stdout and results/join_tuning/<step>[_<config>].json. Needs bench_ch up (SF10
loaded) and the latest SF10 results of both engines.

    python scripts/join_tuning.py failing
    python scripts/join_tuning.py sweep full_sorting_merge
    python scripts/join_tuning.py sweep auto_1.5g
"""
import argparse
import json
import statistics
import subprocess

from check_rewrites import normalize, same_rows
from common import CONTAINERS, RESULTS_DIR, require_only
from run_queries import QUERIES_DIR, cgroup_dir, latest, query_list, timed, wait_ready

SF = 10
FAILING = ["q03", "q05", "q08", "q09", "q21"]
CONFIGS = {
    "default": {},
    "auto_1g": {"join_algorithm": "auto", "max_bytes_in_join": 1_000_000_000},
    "auto_1.5g": {"join_algorithm": "auto", "max_bytes_in_join": 1_500_000_000},
    "grace_hash_1g": {"join_algorithm": "grace_hash", "max_bytes_in_join": 1_000_000_000},
    "partial_merge": {"join_algorithm": "partial_merge"},
    "full_sorting_merge": {"join_algorithm": "full_sorting_merge"},
}
MAX_S = 600  # tighter than the benchmark's 30 min: these are exploratory runs


def by_key(engine: str) -> dict:
    return {(e["query"], e["variant"]): e for e in latest(engine, SF)["queries"]}


def check(ref: dict, q: str, v: str, rows: list) -> str:
    """'ok' if the rows match the Postgres result (or ClickHouse default's, when Postgres has none)."""
    expected = ref.get((q, v), {}).get("result")
    if expected is None:
        return "no reference"
    diff = same_rows([[normalize(x) for x in r] for r in rows], expected)
    return "ok" if diff is None else f"DIFF {diff}"


def short(err: str) -> str:
    return err.split("DB::Exception:")[-1].strip()[:110]


def run_failing(run, cg, ref) -> list[dict]:
    out = []
    for q in FAILING:
        sql = (QUERIES_DIR / "clickhouse" / f"{q}.sql").read_text()
        for name, cfg in CONFIGS.items():
            best = None
            for _ in range(2):
                secs, rows, err, res = timed(run, sql, cg, settings={**cfg, "max_execution_time": MAX_S})
                if err:
                    best = {"status": short(err), "s": round(secs, 2), **res}
                    run = wait_ready("clickhouse", SF)
                    break
                if best is None or secs < best["s"]:
                    best = {"status": check(ref, q, "original", rows), "s": round(secs, 2), **res}
            out.append({"query": q, "config": name, **best})
            print(f"  {q} {name:<19} {best['s']:7.2f}s  anon {best['peak_anon_mb']:7.1f} MB  "
                  f"spill {best['write_mb']:7.1f} MB  cores {best['avg_cores']}  {best['status'][:80]}", flush=True)
    return out


def run_sweep(run, cg, ref, name: str) -> list[dict]:
    cfg = {**CONFIGS[name], "max_execution_time": MAX_S}
    default = by_key("clickhouse")
    out = []
    for q, v, sql in query_list("clickhouse", None):
        if v == "tuned" or (q, v) == ("q21", "original"):  # tuned = this test's subject; Q21 fails under every config
            continue
        secs, rows, err, res = timed(run, sql, cg, settings=cfg)
        entry = {"query": q, "variant": v, "default_warm_median_s": default[(q, v)].get("warm_median_s")}
        if err:
            entry["status"] = short(err)
            run = wait_ready("clickhouse", SF)
        else:
            entry["status"] = check(ref, q, v, rows)
            times, anon = [], []
            for _ in range(3):
                secs, _, _, res = timed(run, sql, cg, settings=cfg)
                times.append(secs)
                anon.append(res["peak_anon_mb"])
            entry["warm_median_s"] = round(statistics.median(times), 3)
            entry["peak_anon_mb"] = max(anon)
            if entry["default_warm_median_s"]:
                entry["x_default"] = round(entry["warm_median_s"] / entry["default_warm_median_s"], 2)
        out.append(entry)
        rel = f"{entry['x_default']:6.2f}x of default" if "x_default" in entry else "default failed   "
        timing = f"{entry['warm_median_s']:7.2f}s  {rel}" if "warm_median_s" in entry else "ERROR"
        print(f"  {q} {v:<9} {timing}  {entry['status'][:80]}", flush=True)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("step", choices=["failing", "sweep"])
    ap.add_argument("config", nargs="?", choices=[c for c in CONFIGS if c != "default"])
    args = ap.parse_args()
    if args.step == "sweep" and not args.config:
        ap.error("sweep needs a config")
    require_only("clickhouse")
    ref = by_key("postgres")
    cg = cgroup_dir("clickhouse")
    subprocess.run(["docker", "restart", CONTAINERS["clickhouse"]], check=True, capture_output=True)
    run = wait_ready("clickhouse", SF)
    if args.step == "failing":
        out, path = run_failing(run, cg, ref), "failing"
    else:
        out, path = run_sweep(run, cg, ref, args.config), f"sweep_{args.config}"
    dest = RESULTS_DIR / "join_tuning" / f"{path}.json"
    dest.parent.mkdir(exist_ok=True)
    dest.write_text(json.dumps(out, indent=1))
    print(f"saved {dest.relative_to(RESULTS_DIR.parent)}")


if __name__ == "__main__":
    main()
