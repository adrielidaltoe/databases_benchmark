"""Write benchmark: applies the committed workload in workloads/sf<N>/ (inserts,
late corrections, deletes, upserts) plus a bulk restatement to one engine, and
measures each run. See README section 7.

Methods:
  postgres     UPDATE / DELETE / COPY + INSERT / MERGE, each run in one transaction
  lightweight  ClickHouse MergeTree: lightweight UPDATE (patch parts) and
               lightweight DELETE (row mask); upsert = DELETE + INSERT
  replacing    ClickHouse ReplacingMergeTree(_version, _is_deleted): every
               change is an INSERT of a new row version or of a tombstone;
               reads use final=1
Both ClickHouse methods apply a restatement with REPLACE PARTITION.

The writes go to copies of orders and lineitem (Postgres schema w_sf<N>,
ClickHouse databases w_sf<N>_lw / w_sf<N>_rmt), rebuilt at the start of every
invocation, so the sf<N> data of the read benchmark is never changed.

Phases, in order:
  setup          copy orders + lineitem from sf<N> (Postgres: + PK/indexes,
                 VACUUM ANALYZE, autovacuum off on the copies so maintenance
                 happens only when the benchmark asks for it)
  reads          Q1 and Q6 (1 warm-up + --read-runs runs) - the baseline
  scenarios      every run of every scenario, in workload order; after each
                 scenario the engine "settles": Postgres CHECKPOINT (writes the
                 dirty pages), ClickHouse waits until merges and mutations are
                 idle. The settle cost is reported per scenario.
  reads          Q1 and Q6 again: the read penalty of pending maintenance
  cleanup        Postgres VACUUM (ANALYZE) + CHECKPOINT; ClickHouse OPTIMIZE
                 TABLE ... FINAL (applies masks, patches and tombstones)
  reads          Q1 and Q6 once more
Checksums of both tables are taken after the writes and after the cleanup;
--compare checks them across methods.

Per run: wall time of all statements of the run (client side), plus the
container's CPU, peak memory, disk read/write (cgroup, as in run_queries.py),
and the rows/bytes the engine read and wrote (Postgres pg_stat_* deltas,
ClickHouse system.query_log). Errors and timeouts are recorded as results.

    python scripts/run_writes.py --engine postgres --sf 1
    python scripts/run_writes.py --engine clickhouse --sf 1 --method lightweight --runs 2
    python scripts/run_writes.py --compare --sf 1
"""
import argparse
import csv
import io
import json
import statistics
import time
from datetime import date, datetime, timezone

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from check_rewrites import CH_CONN, PG_DSN, TIMEOUT_S, normalize
from common import RESULTS_DIR, ROOT, SCALE_FACTORS, read_sql, require_only
from gen_workload import WORKLOAD_DIR
from run_queries import (QUERIES_DIR, RESOURCE_KEYS, ResourceMonitor, cgroup_dir, classify, p95,
                         settings_snapshot)

METHODS = {"postgres": ["postgres"], "clickhouse": ["lightweight", "replacing"]}
READ_QUERIES = ["q01", "q06"]
# Bulk restatement: every line shipped in the year gets a tax rate 1 point higher
RESTATE_YEARS = [1993, 1994, 1995]
SCENARIOS = ["insert_batch", "insert_small", "update_point", "update_100", "update_1k",
             "update_10k", "delete_point", "delete_100", "delete_1k", "upsert_1k", "restatement"]
SOURCE = {  # scenario prefix -> workload files it reads
    "insert": ["insert_orders", "insert_lineitem"],
    "update": ["update_lineitem"],
    "delete": ["delete_orders", "delete_lineitem"],
    "upsert": ["upsert_lineitem"],
}
ORDERS_COLS = ["o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate",
               "o_orderpriority", "o_clerk", "o_shippriority", "o_comment"]
LINEITEM_COLS = ["l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity",
                 "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus",
                 "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode",
                 "l_comment"]
# Columns a correction may change (the generator only changes these)
MONEY_COLS = ["l_quantity", "l_extendedprice", "l_discount", "l_tax"]
SUMMARY_KEYS = [*RESOURCE_KEYS, "read_rows", "read_bytes", "written_rows", "written_bytes",
                "updated_rows", "hot_updated_rows", "units"]
CSV_FIELDS = ["timestamp", "engine", "method", "sf", "scenario", "runs", "ok_runs",
              "median_s", "p95_s", "min_s", "total_s", *(f"median_{k}" for k in SUMMARY_KEYS),
              "settle_s", "settle_active_s", "settle_cpu_s", "settle_write_mb",
              "after_total_bytes", "after_index_bytes", "after_dead_rows",
              "after_active_parts", "after_patch_parts"]
CHECKSUM_SQL = {
    "lineitem": "SELECT count(*), sum(l_quantity), sum(l_extendedprice), sum(l_discount), "
                "sum(l_tax) FROM lineitem",
    "orders": "SELECT count(*), sum(o_totalprice) FROM orders",
}


def runs_of(sf: int, scenario: str, max_runs: int | None) -> list[tuple[int, dict]]:
    """[(run_id, {file: arrow table of that run's rows})] for one scenario."""
    if scenario == "restatement":
        years = RESTATE_YEARS[:max_runs] if max_runs else RESTATE_YEARS
        return [(i, {"year": y}) for i, y in enumerate(years, 1)]
    files = SOURCE[scenario.split("_")[0]]
    tables = {f: pq.read_table(WORKLOAD_DIR / f"sf{sf}" / f"{f}.parquet",
                               filters=[("scenario", "=", scenario)]) for f in files}
    run_ids = sorted(set(tables[files[0]]["run_id"].to_pylist()))
    if max_runs:
        run_ids = run_ids[:max_runs]
    return [(r, {f: t.filter(pc.equal(t["run_id"], r)) for f, t in tables.items()})
            for r in run_ids]


def units(scenario: str, data: dict) -> int:
    """Rows the run changes in its main table (orders for inserts and deletes)."""
    if scenario == "restatement":
        return 0  # filled in from the engine's row counts
    first = next(iter(data.values()))
    return first.num_rows


class Postgres:
    """Row store: one transaction per run."""

    name = "postgres"

    def __init__(self, sf: int):
        import psycopg

        self.sf, self.schema = sf, f"w_sf{sf}"
        self.conn = psycopg.connect(PG_DSN, autocommit=True)
        self.conn.execute(f"SET statement_timeout = '{TIMEOUT_S}s'")

    def setup(self) -> None:
        c, src = self.conn, f"sf{self.sf}"
        c.execute(f"DROP SCHEMA IF EXISTS {self.schema} CASCADE")
        c.execute(f"CREATE SCHEMA {self.schema}")
        c.execute(f"SET search_path TO {self.schema}")
        for t in ("orders", "lineitem"):
            c.execute(f"CREATE TABLE {t} (LIKE {src}.{t}) WITH (autovacuum_enabled = false)")
            c.execute(f"INSERT INTO {t} SELECT * FROM {src}.{t}")
        for stmt in read_sql("postgres_indexes.sql"):
            if any(f" {t} " in f" {stmt} " for t in ("orders", "lineitem")):
                c.execute(stmt)
        c.execute("VACUUM (ANALYZE) orders, lineitem")
        c.execute("CHECKPOINT")

    # -- one run of each scenario -------------------------------------------
    def apply(self, scenario: str, data: dict) -> int | None:
        c = self.conn
        if scenario == "insert_batch":
            with c.transaction():
                self._copy("orders", data["insert_orders"], ORDERS_COLS)
                self._copy("lineitem", data["insert_lineitem"], LINEITEM_COLS)
        elif scenario == "insert_small":
            lines = data["insert_lineitem"]
            for o in data["insert_orders"].select(ORDERS_COLS).to_pylist():
                mine = lines.filter(pc.equal(lines["l_orderkey"], o["o_orderkey"]))
                with c.transaction():
                    c.execute(f"INSERT INTO orders VALUES {pg_row(o.values())}")
                    c.execute("INSERT INTO lineitem VALUES " + ", ".join(
                        pg_row(r.values()) for r in mine.select(LINEITEM_COLS).to_pylist()))
        elif scenario.startswith("update"):
            rows = data["update_lineitem"].select(["l_orderkey", "l_linenumber", *MONEY_COLS])
            c.execute(f"""
                UPDATE lineitem l SET {', '.join(f'{m} = v.{m}' for m in MONEY_COLS)}
                FROM (VALUES {', '.join(pg_row(r.values()) for r in rows.to_pylist())})
                     AS v(l_orderkey, l_linenumber, {', '.join(MONEY_COLS)})
                WHERE l.l_orderkey = v.l_orderkey AND l.l_linenumber = v.l_linenumber""")
        elif scenario.startswith("delete"):
            keys = ", ".join(str(k) for k in data["delete_orders"]["o_orderkey"].to_pylist())
            with c.transaction():
                c.execute(f"DELETE FROM lineitem WHERE l_orderkey IN ({keys})")
                c.execute(f"DELETE FROM orders WHERE o_orderkey IN ({keys})")
        elif scenario.startswith("upsert"):
            rows = data["upsert_lineitem"].select(LINEITEM_COLS).to_pylist()
            rest = [col for col in LINEITEM_COLS if col not in ("l_orderkey", "l_linenumber")]
            c.execute(f"""
                MERGE INTO lineitem l
                USING (VALUES {', '.join(pg_row(r.values()) for r in rows)})
                      AS v({', '.join(LINEITEM_COLS)})
                ON l.l_orderkey = v.l_orderkey AND l.l_linenumber = v.l_linenumber
                WHEN MATCHED THEN UPDATE SET {', '.join(f'{col} = v.{col}' for col in rest)}
                WHEN NOT MATCHED THEN INSERT VALUES ({', '.join(f'v.{col}' for col in LINEITEM_COLS)})""")
        elif scenario == "restatement":
            y = data["year"]
            cur = c.execute(f"UPDATE lineitem SET l_tax = l_tax + 0.01 WHERE l_shipdate >= "
                            f"DATE '{y}-01-01' AND l_shipdate < DATE '{y + 1}-01-01'")
            return cur.rowcount
        return None

    def _copy(self, table: str, rows: pa.Table, cols: list[str]) -> None:
        buf = io.BytesIO()
        pacsv.write_csv(rows.select(cols), buf, pacsv.WriteOptions(include_header=False))
        with self.conn.cursor() as cur, cur.copy(f"COPY {table} FROM STDIN (FORMAT csv)") as cp:
            cp.write(buf.getbuffer())

    # -- maintenance, measurement -------------------------------------------
    def settle(self) -> None:
        self.conn.execute("CHECKPOINT")

    def cleanup(self) -> None:
        self.conn.execute("VACUUM (ANALYZE) orders, lineitem")
        self.conn.execute("CHECKPOINT")

    def query(self, sql: str) -> list:
        return self.conn.execute(sql).fetchall()

    def counters(self) -> dict:
        self.conn.execute("SELECT pg_stat_force_next_flush()")
        r = self.conn.execute("""
            SELECT sum(coalesce(seq_tup_read, 0) + coalesce(idx_tup_fetch, 0)),
                   sum(n_tup_ins), sum(n_tup_upd), sum(n_tup_hot_upd), sum(n_tup_del),
                   (SELECT sum(heap_blks_read + heap_blks_hit + coalesce(idx_blks_read, 0)
                               + coalesce(idx_blks_hit, 0)) * 8192
                    FROM pg_statio_user_tables WHERE schemaname = %(s)s)
            FROM pg_stat_user_tables WHERE schemaname = %(s)s""", {"s": self.schema}).fetchone()
        return dict(zip(["read_rows", "inserted_rows", "updated_rows", "hot_updated_rows",
                         "deleted_rows", "read_bytes"], (int(v or 0) for v in r)))

    def measure(self, fn):
        """Run fn() and return (result, engine-side counters) for the statements it ran."""
        before = self.counters()
        out = fn()
        after = self.counters()
        return out, {k: after[k] - before[k] for k in after}

    def storage(self) -> dict:
        r = self.conn.execute("""
            SELECT sum(pg_total_relation_size(relid)), sum(pg_indexes_size(relid)),
                   sum(n_live_tup), sum(n_dead_tup)
            FROM pg_stat_user_tables WHERE schemaname = %s""", (self.schema,)).fetchone()
        return dict(zip(["total_bytes", "index_bytes", "live_rows", "dead_rows"],
                        (int(v or 0) for v in r)))

    def read_settings(self) -> dict | None:
        return None


class ClickHouse:
    """Columnar: lightweight UPDATE/DELETE on MergeTree, or ReplacingMergeTree."""

    def __init__(self, sf: int, method: str):
        import clickhouse_connect

        self.sf, self.name = sf, method
        self.replacing = method == "replacing"
        self.db = f"w_sf{sf}_{'rmt' if self.replacing else 'lw'}"
        self.version = 0  # ReplacingMergeTree row version; the loaded data is version 0
        base = {"max_execution_time": TIMEOUT_S, "use_query_cache": 0,
                # the update and delete statements carry their keys as literals
                "max_query_size": 100_000_000, "max_ast_elements": 10_000_000,
                "max_expanded_ast_elements": 10_000_000}
        admin = clickhouse_connect.get_client(**CH_CONN, settings=base)
        admin.command(f"DROP DATABASE IF EXISTS {self.db} SYNC")
        admin.command(f"CREATE DATABASE {self.db}")
        self.client = clickhouse_connect.get_client(
            **CH_CONN, database=self.db, send_receive_timeout=TIMEOUT_S + 60, settings=base)
        self.tag = None

    def _settings(self, extra: dict | None = None) -> dict:
        return {"log_comment": self.tag, **(extra or {})} if self.tag else (extra or {})

    def command(self, sql: str, settings: dict | None = None):
        return self.client.command(sql, settings=self._settings(settings))

    def insert(self, table: str, rows: pa.Table) -> None:
        self.client.insert_arrow(table, rows, settings=self._settings())

    def setup(self) -> None:
        ddl = {s.split()[2]: s for s in read_sql("clickhouse_tables.sql")
               if s.split()[2] in ("orders", "lineitem")}
        for t, stmt in ddl.items():
            if self.replacing:
                stmt = stmt.replace(
                    ") ENGINE = MergeTree",
                    ",\n    _version UInt64 DEFAULT 0,\n    _is_deleted UInt8 DEFAULT 0\n"
                    ") ENGINE = ReplacingMergeTree(_version, _is_deleted)")
                extra = ", 0, 0"
            else:
                # lightweight UPDATE needs the row's block number and offset stored
                stmt += " SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1"
                extra = ""
            self.command(stmt)
            self.command(f"INSERT INTO {t} SELECT *{extra} FROM sf{self.sf}.{t}")
        self.settle()

    # -- one run of each scenario -------------------------------------------
    def _versioned(self, rows: pa.Table, cols: list[str], deleted: bool = False) -> pa.Table:
        self.version += 1
        rows = rows.select(cols)
        n = rows.num_rows
        rows = rows.append_column("_version", pa.array([self.version] * n, pa.uint64()))
        return rows.append_column("_is_deleted", pa.array([int(deleted)] * n, pa.uint8()))

    def _write(self, table: str, rows: pa.Table, cols: list[str]) -> None:
        self.insert(table, self._versioned(rows, cols) if self.replacing else rows.select(cols))

    def apply(self, scenario: str, data: dict) -> int | None:
        if scenario == "insert_batch":
            self._write("orders", data["insert_orders"], ORDERS_COLS)
            self._write("lineitem", data["insert_lineitem"], LINEITEM_COLS)
        elif scenario == "insert_small":
            lines = data["insert_lineitem"]
            orders = data["insert_orders"]
            for i in range(orders.num_rows):
                o = orders.slice(i, 1)
                self._write("orders", o, ORDERS_COLS)
                self._write("lineitem", lines.filter(
                    pc.equal(lines["l_orderkey"], o["o_orderkey"][0])), LINEITEM_COLS)
        elif scenario.startswith("update"):
            rows = data["update_lineitem"]
            if self.replacing:
                self._write("lineitem", rows, LINEITEM_COLS)
            else:
                self.command(ch_update_sql(rows))
        elif scenario.startswith("delete"):
            orders, lines = data["delete_orders"], data["delete_lineitem"]
            if self.replacing:  # tombstones: only the sort key matters
                self.insert("lineitem", self._versioned(
                    lines, ["l_orderkey", "l_linenumber", "l_shipdate"], deleted=True))
                self.insert("orders", self._versioned(
                    orders, ["o_orderkey", "o_orderdate"], deleted=True))
            else:
                self.command(f"DELETE FROM lineitem WHERE (l_shipdate, l_orderkey, l_linenumber) "
                             f"IN ({ch_keys(lines, ['l_shipdate', 'l_orderkey', 'l_linenumber'])})")
                self.command(f"DELETE FROM orders WHERE (o_orderdate, o_orderkey) "
                             f"IN ({ch_keys(orders, ['o_orderdate', 'o_orderkey'])})")
        elif scenario.startswith("upsert"):
            rows = data["upsert_lineitem"]
            if not self.replacing:  # no MERGE: delete whatever exists, then insert all
                self.command(f"DELETE FROM lineitem WHERE (l_shipdate, l_orderkey, l_linenumber) "
                             f"IN ({ch_keys(rows, ['l_shipdate', 'l_orderkey', 'l_linenumber'])})")
            self._write("lineitem", rows, LINEITEM_COLS)
        elif scenario == "restatement":
            # rebuild the year in a staging table, then swap it in atomically
            y = data["year"]
            final = " FINAL" if self.replacing else ""
            self.command("DROP TABLE IF EXISTS restate_stage SYNC")
            self.command("CREATE TABLE restate_stage AS lineitem")
            # a bare 0.01 is Float64 in ClickHouse: 0.06 + 0.01 = 0.0699.. would be
            # truncated to 0.06 when stored back into Decimal(15,2)
            self.command("INSERT INTO restate_stage SELECT * REPLACE "
                         "(l_tax + toDecimal32('0.01', 2) AS l_tax) "
                         f"FROM lineitem{final} WHERE toYear(l_shipdate) = {y}")
            n = int(self.command("SELECT count() FROM restate_stage"))
            self.command(f"ALTER TABLE lineitem REPLACE PARTITION {y} FROM restate_stage")
            self.command("DROP TABLE restate_stage SYNC")
            return n
        return None

    # -- maintenance, measurement -------------------------------------------
    def settle(self, quiet_s: float = 3.0) -> float:
        """Wait until no merges or mutations have run for quiet_s seconds; return
        the seconds until the engine went quiet (the background work left over)."""
        t0 = time.monotonic()
        deadline = t0 + TIMEOUT_S
        quiet_since = None
        while time.monotonic() < deadline:
            busy = int(self.client.command(
                "SELECT (SELECT count() FROM system.merges WHERE database = {db:String}) + "
                "(SELECT count() FROM system.mutations WHERE database = {db:String} "
                "AND NOT is_done)", parameters={"db": self.db}))
            if busy:
                quiet_since = None
            elif quiet_since is None:
                quiet_since = time.monotonic()
            elif time.monotonic() - quiet_since >= quiet_s:
                break
            time.sleep(0.5)
        return round((quiet_since or time.monotonic()) - t0, 3)

    def cleanup(self) -> None:
        for t in ("orders", "lineitem"):
            self.command(f"OPTIMIZE TABLE {t} FINAL")
        self.settle()

    def query(self, sql: str) -> list:
        return self.client.query(sql.rstrip().rstrip(";"),
                                 settings=self._settings(self.read_settings())).result_rows

    def measure(self, fn):
        self.tag = f"writes-{time.time_ns()}"
        try:
            out = fn()
        finally:
            tag, self.tag = self.tag, None
        try:
            self.client.command("SYSTEM FLUSH LOGS")
            r = self.client.query(
                "SELECT sum(read_rows), sum(read_bytes), sum(written_rows), sum(written_bytes), "
                "count() FROM system.query_log WHERE log_comment = {t:String} "
                "AND type IN ('QueryFinish', 'ExceptionWhileProcessing')",
                parameters={"t": tag}).result_rows[0]
            stats = dict(zip(["read_rows", "read_bytes", "written_rows", "written_bytes",
                              "statements"], (int(v) for v in r)))
        except Exception:
            stats = {}
        return out, stats

    def storage(self) -> dict:
        r = self.client.query("""
            SELECT sum(bytes_on_disk), count(), countIf(startsWith(name, 'patch-')),
                   sumIf(rows, NOT startsWith(name, 'patch-'))
            FROM system.parts WHERE database = {db:String} AND active""",
                              parameters={"db": self.db}).result_rows[0]
        return dict(zip(["total_bytes", "active_parts", "patch_parts", "stored_rows"],
                        (int(v) for v in r)))

    def read_settings(self) -> dict | None:
        return {"final": 1} if self.replacing else None


def pg_lit(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    if isinstance(v, date):
        return f"DATE '{v.isoformat()}'"
    return str(v)


def pg_row(values) -> str:
    return "(" + ", ".join(pg_lit(v) for v in values) + ")"


def ch_lit(v) -> str:
    if isinstance(v, date):
        return f"toDate('{v.isoformat()}')"
    return str(v)


def ch_keys(rows: pa.Table, cols: list[str]) -> str:
    return ", ".join("(" + ", ".join(ch_lit(r[c]) for c in cols) + ")"
                     for r in rows.select(cols).to_pylist())


def ch_update_sql(rows: pa.Table) -> str:
    """One lightweight UPDATE for a whole correction batch. ClickHouse UPDATE has
    no FROM/join, so each money column is set with transform(row key -> new value);
    the WHERE uses the full sort key so only the granules holding the rows are read."""
    keys = ", ".join(str(k * 8 + n) for k, n in zip(rows["l_orderkey"].to_pylist(),
                                                     rows["l_linenumber"].to_pylist()))
    sets = []
    for col in MONEY_COLS:
        vals = ", ".join(f"'{v}'" for v in rows[col].to_pylist())
        sets.append(f"{col} = transform(l_orderkey * 8 + l_linenumber, CAST([{keys}] AS "
                    f"Array(Int64)), CAST([{vals}] AS Array(Decimal(15, 2))), {col})")
    return (f"UPDATE lineitem SET {', '.join(sets)} WHERE (l_shipdate, l_orderkey, l_linenumber) "
            f"IN ({ch_keys(rows, ['l_shipdate', 'l_orderkey', 'l_linenumber'])})")


def timed(eng, fn, cgroup) -> dict:
    """Run fn() once on the engine; wall time, container resources, engine counters."""
    with ResourceMonitor(cgroup) as mon:
        t0 = time.perf_counter()
        try:
            out, stats = eng.measure(fn)
            err = None
        except Exception as e:  # timeouts / memory limits are results, not crashes
            out, stats, err = None, {}, str(e).splitlines()[0][:300]
        secs = time.perf_counter() - t0
    res = mon.result
    res["avg_cores"] = round(res["cpu_s"] / secs, 2) if secs else None
    entry = {"s": round(secs, 4), **res, **stats}
    if err:
        entry["status"], entry["error"] = classify(err), err
    if isinstance(out, int):
        entry["units"] = out
    elif isinstance(out, float):  # ClickHouse settle: seconds of background work
        entry["active_s"] = out
    return entry


def summarize(runs: list[dict]) -> dict:
    """Median / p95 / min of the successful runs, plus the per-metric medians."""
    ok = [r for r in runs if "error" not in r]
    out = {"runs": len(runs), "ok_runs": len(ok)}
    if not ok:
        return out
    times = [r["s"] for r in ok]
    out.update(median_s=round(statistics.median(times), 4), p95_s=round(p95(times), 4),
               min_s=round(min(times), 4), total_s=round(sum(times), 3))
    for k in SUMMARY_KEYS:
        vals = [r[k] for r in ok if r.get(k) is not None]
        if vals:
            out[f"median_{k}"] = round(statistics.median(vals), 3)
    return out


def reads(eng, cgroup, engine: str, n: int, label: str) -> list[dict]:
    """Q1 and Q6: one untimed warm-up, then n timed runs."""
    out = []
    for q in READ_QUERIES:
        sql = (QUERIES_DIR / engine / f"{q}.sql").read_text()
        try:
            eng.query(sql)
        except Exception:  # e.g. the shared memory limit; the timed runs record it
            pass
        runs =[timed(eng, lambda: eng.query(sql), cgroup) for _ in range(n)]
        s = summarize(runs)
        out.append({"query": q, "phase": label, **s, "runs_detail": runs})
        print(f"  {q} {label:<14} median {s.get('median_s', float('nan')):8.3f}s  "
              f"p95 {s.get('p95_s', float('nan')):8.3f}s", flush=True)
    return out


def checksum(eng) -> dict:
    """Row count and money totals of both tables (with final=1 on ReplacingMergeTree)."""
    out = {}
    for t, sql in CHECKSUM_SQL.items():
        out[t] = [normalize(v) for v in eng.query(sql)[0]]
    return out


def run_method(engine: str, method: str, sf: int, max_runs: int | None, read_runs: int,
               only: set[str] | None) -> None:
    started = datetime.now(timezone.utc)
    ts = started.strftime("%Y%m%dT%H%M%SZ")
    eng = Postgres(sf) if engine == "postgres" else ClickHouse(sf, method)
    cgroup = cgroup_dir(engine)
    print(f"{engine}/{method} sf{sf}: setup", flush=True)
    t0 = time.perf_counter()
    eng.setup()
    result = {"engine": engine, "method": method, "sf": sf, "timestamp": ts,
              "setup_s": round(time.perf_counter() - t0, 1), "timeout_s": TIMEOUT_S,
              **settings_snapshot(engine, sf), "storage": {"setup": eng.storage()},
              "reads": reads(eng, cgroup, engine, read_runs, "baseline"), "scenarios": []}

    for scenario in SCENARIOS:
        if only and scenario not in only:
            continue
        runs = []
        for run_id, data in runs_of(sf, scenario, max_runs):
            entry = timed(eng, lambda: eng.apply(scenario, data), cgroup)
            entry = {"run_id": run_id, "units": units(scenario, data), **entry}
            runs.append(entry)
        settle = timed(eng, eng.settle, cgroup)
        storage = eng.storage()
        s = summarize(runs)
        result["scenarios"].append({"scenario": scenario, **s, "settle": settle,
                                    "storage_after": storage, "runs_detail": runs})
        failed = f"  FAILED {len(runs) - s['ok_runs']}: {runs[-1].get('error', '')[:80]}" \
            if s["ok_runs"] < len(runs) else ""
        print(f"  {scenario:<13} median {s.get('median_s', float('nan')):8.3f}s  "
              f"p95 {s.get('p95_s', float('nan')):8.3f}s  settle {settle.get('active_s', settle['s']):6.1f}s  "
              f"size {storage['total_bytes'] / 2**20:8.1f} MB{failed}", flush=True)

    result["checksum_after_writes"] = checksum(eng)
    result["reads"] += reads(eng, cgroup, engine, read_runs, "after_writes")
    result["cleanup"] = timed(eng, eng.cleanup, cgroup)
    result["storage"]["after_cleanup"] = eng.storage()
    print(f"  cleanup {result['cleanup']['s']:.1f}s  size "
          f"{result['storage']['after_cleanup']['total_bytes'] / 2**20:.1f} MB", flush=True)
    result["reads"] += reads(eng, cgroup, engine, read_runs, "after_cleanup")
    result["checksum_after_cleanup"] = checksum(eng)
    if result["checksum_after_writes"] != result["checksum_after_cleanup"]:
        print("  WARNING: checksum changed during cleanup")
    save(result)


def save(data: dict) -> None:
    """Full run -> results/writes_<engine>_<method>_sf<N>_<ts>.json; one line per
    scenario, cleanup and read phase -> results/writes.csv."""
    RESULTS_DIR.mkdir(exist_ok=True)
    key = f"{data['engine']}_{data['method']}_sf{data['sf']}"
    path = RESULTS_DIR / f"writes_{key}_{data['timestamp']}.json"
    path.write_text(json.dumps(data, indent=1, default=str))

    base = {k: data[k] for k in ("timestamp", "engine", "method", "sf")}
    rows = []
    for s in data["scenarios"]:
        rows.append({**base, **{k: v for k, v in s.items() if not isinstance(v, (dict, list))},
                     **{f"settle_{k}": s["settle"].get(k)
                        for k in ("s", "active_s", "cpu_s", "write_mb")},
                     **{f"after_{k}": v for k, v in s["storage_after"].items()}})
    c = data["cleanup"]
    rows.append({**base, "scenario": "cleanup", "runs": 1, "ok_runs": int("error" not in c),
                 "median_s": c["s"], "median_cpu_s": c["cpu_s"], "median_write_mb": c["write_mb"],
                 **{f"after_{k}": v for k, v in data["storage"]["after_cleanup"].items()}})
    for r in data["reads"]:
        rows.append({**base, "scenario": f"read_{r['query']}_{r['phase']}",
                     **{k: v for k, v in r.items() if not isinstance(v, (dict, list))}})
    summary = RESULTS_DIR / "writes.csv"
    new = not summary.exists()
    with summary.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        if new:
            w.writeheader()
        w.writerows(rows)
    print(f"saved {path.relative_to(ROOT)}")


def compare(sf: int) -> None:
    """Check that every method ended with the same table contents."""
    sums = {}
    for engine, methods in METHODS.items():
        for m in methods:
            paths = sorted(RESULTS_DIR.glob(f"writes_{engine}_{m}_sf{sf}_*.json"))
            if paths:
                d = json.loads(paths[-1].read_text())
                failed = sum(s["runs"] - s["ok_runs"] for s in d["scenarios"])
                sums[m] = (d["checksum_after_cleanup"], [s["scenario"] for s in d["scenarios"]],
                           max((len(s["runs_detail"]) for s in d["scenarios"]), default=0), failed)
    if not sums:
        raise SystemExit(f"no results/writes_*_sf{sf}_*.json yet")
    ref_name, ref = next(iter(sums.items()))
    for m, (cs, scen, n, failed) in sums.items():
        if failed or ref[3]:
            verdict = "not comparable (failed runs)"
        elif (scen, n) != (ref[1], ref[2]):
            verdict = "not comparable (different scenarios or run counts)"
        else:
            verdict = "MATCH" if cs == ref[0] else "DIFF"
        print(f"  {m:<12} {verdict}  {cs}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sf", type=int, choices=SCALE_FACTORS, required=True)
    ap.add_argument("--engine", choices=list(METHODS))
    ap.add_argument("--method", help="ClickHouse only: lightweight or replacing (default both)")
    ap.add_argument("--runs", type=int, help="only the first N runs of each scenario (smoke test)")
    ap.add_argument("--read-runs", type=int, default=5, help="timed runs of Q1/Q6 per phase")
    ap.add_argument("--scenarios", help="comma-separated subset, e.g. update_1k,delete_100")
    ap.add_argument("--compare", action="store_true", help="compare final checksums of all methods")
    args = ap.parse_args()
    if args.compare:
        return compare(args.sf)
    if not args.engine:
        ap.error("give --engine or --compare")
    methods = METHODS[args.engine]
    if args.method:
        if args.method not in methods:
            ap.error(f"--method must be one of {methods}")
        methods = [args.method]
    require_only(args.engine)
    only = set(args.scenarios.split(",")) if args.scenarios else None
    for m in methods:
        run_method(args.engine, m, args.sf, args.runs, args.read_runs, only)


if __name__ == "__main__":
    main()
