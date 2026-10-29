"""Generate the write workload (inserts, corrections, deletes, upserts) for one
scale factor and save it as Parquet in workloads/sf<N>/, next to a manifest.

The files are committed, so every benchmark run applies exactly the same
changes to both engines. Reads data/sf<N>/ (run gen_tpch.py first).

    python scripts/gen_workload.py --sf 1

Every random choice is a hash of (row key, purpose, SEED), not a stateful RNG,
so the output does not depend on thread count or row order and the same SEED
always gives the same files.
"""
import argparse
import hashlib
import json
import time

import duckdb

from common import ROOT, SCALE_FACTORS, fmt_bytes, parquet_path

WORKLOAD_DIR = ROOT / "workloads"
SEED = 20261029
RUNS = 10

# TPC-H dates (spec 4.2.3): CURRENTDATE decides return flag and line status
CURRENT_DATE = "1995-06-17"
# New orders model a daily load: order dates in the last 30 days of the TPC-H
# range (which ends 1998-08-02), so each insert lands in a single year partition
NEW_ORDER_FIRST_DATE = "1998-07-04"
NEW_ORDER_DAYS = 30

# Scenario -> units per run. Units are lineitems for updates and upserts,
# orders (with all their lineitems) for inserts and deletes.
UPDATES = {"update_point": 1, "update_100": 100, "update_1k": 1_000, "update_10k": 10_000}
DELETES = {"delete_point": 1, "delete_100": 100, "delete_1k": 1_000}
UPSERT_ROWS = 1_000  # half corrections of existing lines, half new lines


def inserts(sf: int) -> dict[str, int]:
    # insert_batch is the TPC-H RF1 size (0.1% of orders) sent as one statement;
    # insert_small is meant to be sent one order per statement
    return {"insert_batch": 1_500 * sf, "insert_small": 100}


def plan(sizes: dict[str, int]) -> list[tuple[str, int, int, int]]:
    """Split a ranked list of units into (scenario, run_id, first_rank, last_rank)
    blocks: every scenario gets RUNS consecutive blocks of its size."""
    rows, start = [], 0
    for scenario, size in sizes.items():
        for run in range(1, RUNS + 1):
            rows.append((scenario, run, start, start + size - 1))
            start += size
    return rows


def register_plan(con, name: str, sizes: dict[str, int]) -> int:
    rows = plan(sizes)
    con.execute(f"CREATE OR REPLACE TEMP TABLE {name} "
                "(scenario VARCHAR, run_id INTEGER, lo BIGINT, hi BIGINT)")
    con.executemany(f"INSERT INTO {name} VALUES (?, ?, ?, ?)", rows)
    return rows[-1][3] + 1


def new_lines_sql(src: str, sf: int) -> str:
    """Lineitem columns for (scenario, run_id, l_orderkey, l_linenumber,
    o_orderdate) rows in `src`, following the TPC-H spec 4.2.3 formulas
    (supplier and retail price checked against the dbgen output)."""
    s = sf * 10_000
    return f"""
    WITH k AS (
        SELECT *, ri(l_orderkey, l_linenumber, 'partkey', 1, {sf * 200_000}) AS l_partkey,
               ri(l_orderkey, l_linenumber, 'quantity', 1, 50) AS qty,
               o_orderdate + ri(l_orderkey, l_linenumber, 'ship', 1, 121)::INTEGER AS l_shipdate
        FROM {src}
    )
    SELECT scenario, run_id, l_orderkey,
        l_partkey,
        (l_partkey + ri(l_orderkey, l_linenumber, 'supp', 0, 3)
            * ({s // 4} + (l_partkey - 1) // {s})) % {s} + 1 AS l_suppkey,
        l_linenumber,
        qty::DECIMAL(15,2) AS l_quantity,
        (qty * (90000 + ((l_partkey // 10) % 20001) + 100 * (l_partkey % 1000)) / 100)
            ::DECIMAL(15,2) AS l_extendedprice,
        (ri(l_orderkey, l_linenumber, 'discount', 0, 10) / 100)::DECIMAL(15,2) AS l_discount,
        (ri(l_orderkey, l_linenumber, 'tax', 0, 8) / 100)::DECIMAL(15,2) AS l_tax,
        CASE WHEN l_receiptdate > DATE '{CURRENT_DATE}' THEN 'N'
             WHEN ri(l_orderkey, l_linenumber, 'return', 0, 1) = 0 THEN 'R' ELSE 'A' END
            AS l_returnflag,
        CASE WHEN l_shipdate > DATE '{CURRENT_DATE}' THEN 'O' ELSE 'F' END AS l_linestatus,
        l_shipdate,
        o_orderdate + ri(l_orderkey, l_linenumber, 'commit', 30, 90)::INTEGER AS l_commitdate,
        l_receiptdate,
        (['DELIVER IN PERSON', 'COLLECT COD', 'NONE', 'TAKE BACK RETURN'])
            [ri(l_orderkey, l_linenumber, 'instruct', 1, 4)] AS l_shipinstruct,
        (['REG AIR', 'AIR', 'RAIL', 'SHIP', 'TRUCK', 'MAIL', 'FOB'])
            [ri(l_orderkey, l_linenumber, 'mode', 1, 7)] AS l_shipmode,
        (SELECT c FROM l_comments WHERE i = ri(l_orderkey, l_linenumber, 'comment', 0, 9999))
            AS l_comment
    FROM (SELECT *, l_shipdate + ri(l_orderkey, l_linenumber, 'receipt', 1, 30)::INTEGER
                    AS l_receiptdate FROM k)
    """


def corrections_sql(src: str) -> str:
    """Correct one money column of each lineitem in `src` (full lineitem rows
    plus scenario, run_id). The new value always differs from the old one, and
    the ORDER BY key columns (l_shipdate, l_orderkey, l_linenumber) never change."""
    return f"""
    WITH t AS (
        SELECT *, (['discount', 'tax', 'quantity'])
                  [ri(l_orderkey, l_linenumber, 'correction', 1, 3)] AS correction_type,
               ri(l_orderkey, l_linenumber, 'shift', 1, 49) AS shift
        FROM {src}
    ), q AS (
        SELECT *, CASE WHEN correction_type = 'quantity'
                       THEN (l_quantity::INTEGER - 1 + shift) % 50 + 1
                       ELSE l_quantity::INTEGER END AS new_qty
        FROM t
    )
    SELECT scenario, run_id, correction_type,
        l_orderkey, l_partkey, l_suppkey, l_linenumber,
        new_qty::DECIMAL(15,2) AS l_quantity,
        -- extendedprice = quantity * retail price, so rescale it with quantity
        (new_qty * (l_extendedprice / l_quantity)::DECIMAL(15,2))::DECIMAL(15,2)
            AS l_extendedprice,
        CASE WHEN correction_type = 'discount'
             THEN (((l_discount * 100)::INTEGER + 1 + shift % 10) % 11 / 100)::DECIMAL(15,2)
             ELSE l_discount END AS l_discount,
        CASE WHEN correction_type = 'tax'
             THEN (((l_tax * 100)::INTEGER + 1 + shift % 8) % 9 / 100)::DECIMAL(15,2)
             ELSE l_tax END AS l_tax,
        l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate,
        l_shipinstruct, l_shipmode, l_comment
    FROM q
    """


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sf", type=int, choices=SCALE_FACTORS, required=True)
    sf = ap.parse_args().sf

    out_dir = WORKLOAD_DIR / f"sf{sf}"
    out_dir.mkdir(parents=True, exist_ok=True)
    orders, lineitem = parquet_path(sf, "orders"), parquet_path(sf, "lineitem")

    con = duckdb.connect()
    con.execute("SET memory_limit = '2GB'")
    con.execute("SET threads = 3")
    con.execute(f"CREATE MACRO h(a, b, salt) AS hash(a, b, salt, {SEED})")
    con.execute("CREATE MACRO ri(a, b, salt, lo, hi) AS "
                "(lo + h(a, b, salt) % (hi - lo + 1))::BIGINT")
    # Comment pools sampled from the real data, so new rows have realistic text
    for table, col, key in [("lineitem", "l_comment", "l_orderkey, l_linenumber"),
                            ("orders", "o_comment", "o_orderkey, 0")]:
        con.execute(f"""
            CREATE TEMP TABLE {col[0]}_comments AS
            SELECT row_number() OVER (ORDER BY h({key}, 'pool')) - 1 AS i, {col} AS c
            FROM (SELECT * FROM '{parquet_path(sf, table)}'
                  ORDER BY h({key}, 'pool') LIMIT 10000)""")

    t0 = time.perf_counter()

    # Deletes: whole orders (a reversal removes the order and all its lines)
    n = register_plan(con, "delete_plan", DELETES)
    con.execute(f"""
        CREATE TEMP TABLE delete_orders AS
        SELECT p.scenario, p.run_id, r.o_orderkey, r.o_orderdate
        FROM (SELECT o_orderkey, o_orderdate,
                     row_number() OVER (ORDER BY h(o_orderkey, 0, 'delete'), o_orderkey) - 1 AS rn
              FROM (SELECT * FROM '{orders}' ORDER BY h(o_orderkey, 0, 'delete'), o_orderkey
                    LIMIT {n})) r
        JOIN delete_plan p ON r.rn BETWEEN p.lo AND p.hi""")
    con.execute(f"""
        CREATE TEMP TABLE delete_lineitem AS
        SELECT d.scenario, d.run_id, l.l_orderkey, l.l_linenumber, l.l_shipdate
        FROM '{lineitem}' l JOIN delete_orders d ON l.l_orderkey = d.o_orderkey""")

    # Corrections: one ranking of lineitems (outside deleted orders), split into
    # the update scenarios and the existing half of the upserts
    sizes = {**UPDATES, "upsert_1k": UPSERT_ROWS // 2}
    n = register_plan(con, "fix_plan", sizes)
    con.execute(f"""
        CREATE TEMP TABLE fix_src AS
        SELECT p.scenario, p.run_id, r.* EXCLUDE (rn)
        FROM (SELECT *, row_number() OVER (ORDER BY h(l_orderkey, l_linenumber, 'fix'),
                                           l_orderkey, l_linenumber) - 1 AS rn
              FROM (SELECT * FROM '{lineitem}'
                    WHERE l_orderkey NOT IN (SELECT o_orderkey FROM delete_orders)
                    ORDER BY h(l_orderkey, l_linenumber, 'fix'), l_orderkey, l_linenumber
                    LIMIT {n})) r
        JOIN fix_plan p ON r.rn BETWEEN p.lo AND p.hi""")
    con.execute(f"CREATE TEMP TABLE fixes AS {corrections_sql('fix_src')}")

    # Upserts, new half: a late line added to an existing order with < 7 lines
    n = register_plan(con, "late_plan", {"upsert_1k": UPSERT_ROWS - UPSERT_ROWS // 2})
    con.execute(f"""
        CREATE TEMP TABLE late_src AS
        SELECT p.scenario, p.run_id, r.l_orderkey, r.l_linenumber, r.o_orderdate
        FROM (SELECT *, row_number() OVER (ORDER BY h(l_orderkey, 0, 'late'), l_orderkey) - 1 AS rn
              FROM (SELECT l.l_orderkey, max(l.l_linenumber) + 1 AS l_linenumber, o.o_orderdate
                    FROM '{lineitem}' l JOIN '{orders}' o ON o.o_orderkey = l.l_orderkey
                    WHERE l.l_orderkey NOT IN (SELECT o_orderkey FROM delete_orders)
                    GROUP BY l.l_orderkey, o.o_orderdate
                    HAVING max(l.l_linenumber) < 7
                    ORDER BY h(l.l_orderkey, 0, 'late'), l.l_orderkey
                    LIMIT {n})) r
        JOIN late_plan p ON r.rn BETWEEN p.lo AND p.hi""")
    con.execute(f"""
        CREATE TEMP TABLE upserts AS
        SELECT scenario, run_id, false AS is_new, * EXCLUDE (scenario, run_id)
        FROM fixes WHERE scenario = 'upsert_1k'
        UNION ALL BY NAME
        SELECT scenario, run_id, true AS is_new, NULL::VARCHAR AS correction_type,
               * EXCLUDE (scenario, run_id)
        FROM ({new_lines_sql('late_src', sf)})""")

    # Inserts: new orders on the order keys dbgen leaves unused. dbgen only uses
    # keys with key % 32 in 0..7, so 8..31 are free (TPC-H RF1 does the same).
    n = register_plan(con, "insert_plan", inserts(sf))
    max_key = con.execute(f"SELECT max(o_orderkey) FROM '{orders}'").fetchone()[0]
    con.execute(f"""
        CREATE TEMP TABLE new_orders AS
        SELECT p.scenario, p.run_id, r.o_orderkey,
               -- dbgen never gives orders to customers whose key is a multiple of 3
               c + (c - 1) // 2 AS o_custkey,
               DATE '{NEW_ORDER_FIRST_DATE}'
                   + ri(r.o_orderkey, 0, 'date', 0, {NEW_ORDER_DAYS - 1})::INTEGER AS o_orderdate,
               ri(r.o_orderkey, 0, 'lines', 1, 7) AS n_lines
        FROM (SELECT o_orderkey, ri(o_orderkey, 0, 'cust', 1, {sf * 100_000}) AS c,
                     row_number() OVER (ORDER BY h(o_orderkey, 0, 'insert'), o_orderkey) - 1 AS rn
              FROM (SELECT g * 32 + s AS o_orderkey
                    FROM range({max_key // 32}) t(g), range(8, 32) u(s)
                    ORDER BY h(o_orderkey, 0, 'insert'), o_orderkey
                    LIMIT {n})) r
        JOIN insert_plan p ON r.rn BETWEEN p.lo AND p.hi""")
    con.execute("""
        CREATE TEMP TABLE new_line_src AS
        SELECT scenario, run_id, o_orderkey AS l_orderkey, l_linenumber, o_orderdate
        FROM new_orders, range(1, n_lines + 1) t(l_linenumber)""")
    con.execute(f"CREATE TEMP TABLE insert_lineitem AS {new_lines_sql('new_line_src', sf)}")
    con.execute(f"""
        CREATE TEMP TABLE insert_orders AS
        SELECT o.scenario, o.run_id, o.o_orderkey, o.o_custkey,
               CASE WHEN bool_and(l.l_linestatus = 'F') THEN 'F'
                    WHEN bool_and(l.l_linestatus = 'O') THEN 'O' ELSE 'P' END AS o_orderstatus,
               sum(l.l_extendedprice * (1 + l.l_tax) * (1 - l.l_discount))::DECIMAL(15,2)
                   AS o_totalprice,
               o.o_orderdate,
               (['1-URGENT', '2-HIGH', '3-MEDIUM', '4-NOT SPECIFIED', '5-LOW'])
                   [ri(o.o_orderkey, 0, 'priority', 1, 5)] AS o_orderpriority,
               'Clerk#' || lpad(ri(o.o_orderkey, 0, 'clerk', 1, {sf * 1000})::VARCHAR, 9, '0')
                   AS o_clerk,
               0 AS o_shippriority,
               (SELECT c FROM o_comments WHERE i = ri(o.o_orderkey, 0, 'comment', 0, 9999))
                   AS o_comment
        FROM new_orders o JOIN insert_lineitem l ON l.l_orderkey = o.o_orderkey
        GROUP BY o.scenario, o.run_id, o.o_orderkey, o.o_custkey, o.o_orderdate""")

    outputs = {
        "insert_orders": ("insert_orders", "o_orderkey"),
        "insert_lineitem": ("insert_lineitem", "l_orderkey, l_linenumber"),
        "update_lineitem": ("(SELECT * FROM fixes WHERE scenario <> 'upsert_1k')",
                            "l_orderkey, l_linenumber"),
        "delete_orders": ("delete_orders", "o_orderkey"),
        "delete_lineitem": ("delete_lineitem", "l_orderkey, l_linenumber"),
        "upsert_lineitem": ("upserts", "l_orderkey, l_linenumber"),
    }
    files = {}
    for name, (rel, key) in outputs.items():
        path = out_dir / f"{name}.parquet"
        con.execute(f"COPY (SELECT * FROM {rel} ORDER BY scenario, run_id, {key}) "
                    f"TO '{path}' (FORMAT parquet, COMPRESSION zstd)")
        counts = dict(con.execute(f"SELECT scenario, count(*) FROM {rel} "
                                  "GROUP BY 1 ORDER BY 1").fetchall())
        files[f"{name}.parquet"] = {
            "rows": sum(counts.values()),
            "rows_per_scenario": counts,
            "bytes": path.stat().st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }
        print(f"  {name:<16} {files[f'{name}.parquet']['rows']:>9,} rows  "
              f"{fmt_bytes(path.stat().st_size):>10}")

    manifest = {
        "sf": sf,
        "seed": SEED,
        "runs": RUNS,
        "duckdb_version": duckdb.__version__,
        "units_per_run": {
            "insert (orders)": inserts(sf),
            "update (lineitems)": UPDATES,
            "delete (orders)": DELETES,
            "upsert (lineitems)": {"upsert_1k": UPSERT_ROWS},
        },
        "new_order_dates": [NEW_ORDER_FIRST_DATE, f"+{NEW_ORDER_DAYS} days"],
        "files": files,
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"workload sf={sf}: {time.perf_counter() - t0:.1f}s -> {out_dir}")


if __name__ == "__main__":
    main()
