"""Check that each hand-written CTE rewrite returns the same result as the
original correlated TPC-H query, on one engine at one scale factor.

Runs queries/<engine>/qNN.sql and queries/<engine>/rewritten/qNN.sql once each
(single run - this is a correctness check, not a timed benchmark), compares the
result sets, and saves them to results/check_rewrites_<engine>_sf<N>.json.
--compare then checks the saved Postgres and ClickHouse results against each other.

    python scripts/check_rewrites.py --engine postgres --sf 1
    python scripts/check_rewrites.py --engine clickhouse --sf 1
    python scripts/check_rewrites.py --compare --sf 1
"""
import argparse
import json
import math
import time
from datetime import date, datetime
from decimal import Decimal

from common import RESULTS_DIR, ROOT, SCALE_FACTORS, require_only

QUERIES_DIR = ROOT / "queries"
REWRITTEN = ["q02", "q04", "q17", "q20", "q21", "q22"]
TIMEOUT_S = 1800
# Postgres numeric vs ClickHouse Decimal/Float64 can differ in the last digits
# of averages and divisions.
REL_TOL = 1e-6

PG_DSN = "host=localhost port=5432 dbname=bench user=bench password=bench"
CH_CONN = dict(host="localhost", port=8123, username="bench", password="bench")


def normalize(v):
    """Make values from both drivers comparable and JSON-serializable."""
    if isinstance(v, (Decimal, float)):
        return float(v)
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    if isinstance(v, str):
        return v.rstrip()
    return v


def same_rows(a: list[list], b: list[list]) -> str | None:
    """None if equal (numbers within REL_TOL), else a description of the first difference."""
    if len(a) != len(b):
        return f"row count {len(a)} vs {len(b)}"
    for i, (ra, rb) in enumerate(zip(a, b)):
        if len(ra) != len(rb):
            return f"row {i}: {len(ra)} vs {len(rb)} columns"
        for j, (x, y) in enumerate(zip(ra, rb)):
            if isinstance(x, float) or isinstance(y, float):
                if not math.isclose(x, y, rel_tol=REL_TOL, abs_tol=1e-9):
                    return f"row {i} col {j}: {x!r} vs {y!r}"
            elif x != y:
                return f"row {i} col {j}: {x!r} vs {y!r}"
    return None


def make_runner(engine: str, sf: int):
    """Return run(sql, settings=None) -> rows for the engine, connected to the sf<N>
    schema/database. settings are per-query ClickHouse settings (ignored by Postgres);
    run.handle is the underlying psycopg connection / clickhouse-connect client."""
    if engine == "postgres":
        import psycopg

        conn = psycopg.connect(PG_DSN, autocommit=True)
        conn.execute(f"SET search_path TO sf{sf}")
        conn.execute(f"SET statement_timeout = '{TIMEOUT_S}s'")

        def run(sql, settings=None):
            return conn.execute(sql).fetchall()
        run.handle = conn
    else:
        import clickhouse_connect

        client = clickhouse_connect.get_client(
            **CH_CONN, database=f"sf{sf}", send_receive_timeout=TIMEOUT_S + 60,
            settings={"max_execution_time": TIMEOUT_S, "use_query_cache": 0},
        )

        def run(sql, settings=None):
            return client.query(sql.rstrip().rstrip(";"), settings=settings).result_rows
        run.handle = client
    return run


def run_engine(engine: str, sf: int) -> None:
    require_only(engine)
    run = make_runner(engine, sf)
    out = {}
    for q in REWRITTEN:
        entry = {}
        for variant, path in [("original", QUERIES_DIR / engine / f"{q}.sql"),
                              ("rewritten", QUERIES_DIR / engine / "rewritten" / f"{q}.sql")]:
            t0 = time.perf_counter()
            try:
                rows = [[normalize(v) for v in r] for r in run(path.read_text())]
                entry[variant] = {"seconds": round(time.perf_counter() - t0, 3), "rows": rows}
            except Exception as e:  # timeouts / memory limits are results, not crashes
                entry[variant] = {"seconds": round(time.perf_counter() - t0, 3),
                                  "error": str(e).splitlines()[0][:300]}
        o, r = entry["original"], entry["rewritten"]
        if "rows" in o and "rows" in r:
            diff = same_rows(o["rows"], r["rows"])
            entry["match"] = diff is None
            status = "MATCH" if diff is None else f"DIFF ({diff})"
        else:
            entry["match"] = None
            status = "ERROR: " + (o.get("error") or r.get("error"))
        print(f"  {q}  original {o['seconds']:8.2f}s  rewritten {r['seconds']:8.2f}s  "
              f"rows {len(o.get('rows', [])):>4}  {status}")
        out[q] = entry

    RESULTS_DIR.mkdir(exist_ok=True)
    path = RESULTS_DIR / f"check_rewrites_{engine}_sf{sf}.json"
    path.write_text(json.dumps({"engine": engine, "sf": sf, "queries": out}, indent=1))
    print(f"saved {path.relative_to(ROOT)}")


def compare_engines(sf: int) -> None:
    pg, ch = (json.loads((RESULTS_DIR / f"check_rewrites_{e}_sf{sf}.json").read_text())["queries"]
              for e in ("postgres", "clickhouse"))
    for q in REWRITTEN:
        for variant in ("original", "rewritten"):
            a, b = pg[q][variant], ch[q][variant]
            if "rows" not in a or "rows" not in b:
                print(f"  {q} {variant:<9}  skipped (error on one engine)")
                continue
            diff = same_rows(a["rows"], b["rows"])
            print(f"  {q} {variant:<9}  {'MATCH' if diff is None else 'DIFF (' + diff + ')'}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sf", type=int, choices=SCALE_FACTORS, required=True)
    ap.add_argument("--engine", choices=["postgres", "clickhouse"])
    ap.add_argument("--compare", action="store_true", help="compare saved Postgres vs ClickHouse results")
    args = ap.parse_args()
    if args.compare:
        compare_engines(args.sf)
    elif args.engine:
        run_engine(args.engine, args.sf)
    else:
        ap.error("give --engine or --compare")


if __name__ == "__main__":
    main()
