"""Load TPC-H Parquet files into ClickHouse and record load time + on-disk size.

Tables go into database sf<N> so several scale factors can coexist.
Phases (each timed): create tables -> INSERT (Arrow batches) -> wait for
background merges to settle (the columnar equivalent of index build + VACUUM:
work the engine still has to do after the rows are "in").

    python scripts/load_clickhouse.py --sf 1
"""
import argparse
import time

import clickhouse_connect
import pyarrow as pa
import pyarrow.parquet as pq

from common import (BATCH_ROWS, SCALE_FACTORS, TABLES, Stopwatch, fmt_bytes,
                    parquet_path, read_sql, require_only, save_result)

CONN = dict(host="localhost", port=8123, username="bench", password="bench")


def insert_table(client, table: str, sf: int) -> int:
    rows = 0
    for batch in pq.ParquetFile(parquet_path(sf, table)).iter_batches(batch_size=BATCH_ROWS):
        client.insert_arrow(table, pa.Table.from_batches([batch]))
        rows += batch.num_rows
    return rows


def wait_for_merges(client, db: str, quiet_s: float = 3.0, timeout_s: float = 1800) -> None:
    """Block until no merges have run for `quiet_s` seconds."""
    deadline = time.monotonic() + timeout_s
    quiet_since = None
    while time.monotonic() < deadline:
        running = client.command(
            "SELECT count() FROM system.merges WHERE database = {db:String}", parameters={"db": db})
        if int(running) == 0:
            quiet_since = quiet_since or time.monotonic()
            if time.monotonic() - quiet_since >= quiet_s:
                return
        else:
            quiet_since = None
        time.sleep(0.5)
    print("  WARNING: merges still running after timeout")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sf", type=int, choices=SCALE_FACTORS, required=True)
    sf = ap.parse_args().sf
    db = f"sf{sf}"

    require_only("clickhouse")
    sw = Stopwatch()
    per_table = {}

    admin = clickhouse_connect.get_client(**CONN)
    admin.command(f"DROP DATABASE IF EXISTS {db} SYNC")
    admin.command(f"CREATE DATABASE {db}")
    client = clickhouse_connect.get_client(**CONN, database=db)

    with sw.phase("create_tables"):
        for stmt in read_sql("clickhouse_tables.sql"):
            client.command(stmt)

    for table in TABLES:
        with sw.phase("insert"):
            sw_t = Stopwatch()
            with sw_t.phase("t"):
                rows = insert_table(client, table, sf)
        expected = pq.ParquetFile(parquet_path(sf, table)).metadata.num_rows
        assert rows == expected, f"{table}: loaded {rows}, parquet has {expected}"
        per_table[table] = {"rows": rows, "insert_s": round(sw_t.total, 3)}
        print(f"  INSERT {table:<10} {rows:>12,} rows  {sw_t.total:8.1f}s")

    with sw.phase("merge_settle"):
        wait_for_merges(client, db)

    sizes = client.query(
        """
        SELECT table,
               sum(bytes_on_disk)            AS total_bytes,
               sum(data_compressed_bytes)    AS compressed_bytes,
               sum(data_uncompressed_bytes)  AS uncompressed_bytes,
               sum(primary_key_bytes_in_memory) AS pk_bytes,
               count()                       AS active_parts
        FROM system.parts
        WHERE database = {db:String} AND active
        GROUP BY table
        """,
        parameters={"db": db},
    ).named_results()
    for r in sizes:
        per_table[r["table"]].update({k: int(v) for k, v in r.items() if k != "table"})

    version = client.server_version
    settings = {r[0]: r[1] for r in client.query(
        "SELECT name, value FROM system.settings WHERE changed ORDER BY name").result_rows}
    server_settings = {r[0]: r[1] for r in client.query(
        "SELECT name, value FROM system.server_settings WHERE changed ORDER BY name").result_rows}

    parquet_bytes = sum(parquet_path(sf, t).stat().st_size for t in TABLES)
    total_bytes = sum(t["total_bytes"] for t in per_table.values())
    compressed = sum(t["compressed_bytes"] for t in per_table.values())
    uncompressed = sum(t["uncompressed_bytes"] for t in per_table.values())
    phases = {k: round(v, 3) for k, v in sw.phases.items()}

    path = save_result(
        "clickhouse", sf,
        {"version": version, "settings": settings, "server_settings": server_settings,
         "batch_rows": BATCH_ROWS, "phases_s": phases, "total_load_s": round(sw.total, 3),
         "parquet_bytes": parquet_bytes, "compressed_bytes": compressed,
         "uncompressed_bytes": uncompressed, "tables": per_table},
        # ClickHouse has no separate indexes: sparse PK + marks live inside the parts
        {"total_load_s": round(sw.total, 1), "ingest_s": phases["insert"],
         "post_ingest_s": phases["merge_settle"],
         "data_bytes": total_bytes, "index_bytes": 0, "total_bytes": total_bytes,
         "parquet_bytes": parquet_bytes},
    )

    print(f"\nClickHouse SF{sf}: total load {sw.total:.1f}s  " +
          "  ".join(f"{k}={v:.1f}s" for k, v in phases.items()))
    print(f"  on disk {fmt_bytes(total_bytes)}  (uncompressed {fmt_bytes(uncompressed)}, "
          f"ratio {uncompressed / compressed:.1f}x; {total_bytes / parquet_bytes:.1f}x the zstd Parquet source)")
    print(f"  -> {path.relative_to(path.parents[1])}")


if __name__ == "__main__":
    main()
