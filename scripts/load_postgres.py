"""Load TPC-H Parquet files into PostgreSQL and record load time + on-disk size.

Tables go into schema sf<N> so several scale factors can coexist.
Phases (each timed): create tables -> COPY -> PKs + indexes -> VACUUM ANALYZE
-> CHECKPOINT (so all data is actually on disk, comparable to ClickHouse parts).

    python scripts/load_postgres.py --sf 1
"""
import argparse
import io

import psycopg
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from common import (BATCH_ROWS, SCALE_FACTORS, TABLES, Stopwatch, fmt_bytes,
                    parquet_path, read_sql, require_only, save_result)

DSN = "host=localhost port=5432 dbname=bench user=bench password=bench"


def copy_table(conn: psycopg.Connection, table: str, sf: int) -> int:
    """Stream Parquet -> CSV batches -> COPY FROM STDIN (Postgres has no native Parquet reader)."""
    rows = 0
    opts = pacsv.WriteOptions(include_header=False)
    with conn.cursor() as cur, cur.copy(f"COPY {table} FROM STDIN (FORMAT csv)") as copy:
        for batch in pq.ParquetFile(parquet_path(sf, table)).iter_batches(batch_size=BATCH_ROWS):
            buf = io.BytesIO()
            pacsv.write_csv(batch, buf, opts)
            copy.write(buf.getbuffer())
            rows += batch.num_rows
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sf", type=int, choices=SCALE_FACTORS, required=True)
    sf = ap.parse_args().sf
    schema = f"sf{sf}"

    require_only("postgres")
    sw = Stopwatch()
    per_table, per_index = {}, []

    # autocommit: each COPY commits on its own, and VACUUM can't run in a transaction
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.execute(f"CREATE SCHEMA {schema}")
        conn.execute(f"SET search_path TO {schema}")

        with sw.phase("create_tables"):
            for stmt in read_sql("postgres_tables.sql"):
                conn.execute(stmt)

        for table in TABLES:
            with sw.phase("copy"):
                sw_t = Stopwatch()
                with sw_t.phase("t"):
                    rows = copy_table(conn, table, sf)
            expected = pq.ParquetFile(parquet_path(sf, table)).metadata.num_rows
            assert rows == expected, f"{table}: loaded {rows}, parquet has {expected}"
            per_table[table] = {"rows": rows, "copy_s": round(sw_t.total, 3)}
            print(f"  COPY {table:<10} {rows:>12,} rows  {sw_t.total:8.1f}s")

        with sw.phase("indexes"):
            for stmt in read_sql("postgres_indexes.sql"):
                sw_i = Stopwatch()
                with sw_i.phase("i"):
                    conn.execute(stmt)
                per_index.append({"sql": stmt, "s": round(sw_i.total, 3)})
                print(f"  {sw_i.total:8.1f}s  {stmt}")

        # VACUUM (not just ANALYZE): sets hint bits + visibility map now, so the
        # first benchmark query doesn't pay that cost and index-only scans work
        with sw.phase("vacuum_analyze"):
            conn.execute("VACUUM (ANALYZE)")

        with sw.phase("checkpoint"):
            conn.execute("CHECKPOINT")

        sizes = conn.execute(
            """
            SELECT c.relname,
                   pg_table_size(c.oid)          AS data_bytes,   -- heap + TOAST + FSM/VM
                   pg_indexes_size(c.oid)        AS index_bytes,
                   pg_total_relation_size(c.oid) AS total_bytes
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relkind = 'r'
            """,
            (schema,),
        ).fetchall()
        for name, data_b, idx_b, total_b in sizes:
            per_table[name].update(data_bytes=data_b, index_bytes=idx_b, total_bytes=total_b)

        version = conn.execute("SELECT version()").fetchone()[0]
        settings = dict(conn.execute(
            "SELECT name, setting || coalesce(unit, '') FROM pg_settings "
            "WHERE source NOT IN ('default', 'override') ORDER BY name"
        ).fetchall())

    parquet_bytes = sum(parquet_path(sf, t).stat().st_size for t in TABLES)
    data_bytes = sum(t["data_bytes"] for t in per_table.values())
    index_bytes = sum(t["index_bytes"] for t in per_table.values())
    total_bytes = data_bytes + index_bytes
    phases = {k: round(v, 3) for k, v in sw.phases.items()}

    path = save_result(
        "postgres", sf,
        {"version": version, "settings": settings, "batch_rows": BATCH_ROWS,
         "phases_s": phases, "total_load_s": round(sw.total, 3),
         "parquet_bytes": parquet_bytes, "tables": per_table, "indexes": per_index},
        {"total_load_s": round(sw.total, 1), "ingest_s": phases["copy"],
         "post_ingest_s": round(sw.total - phases["copy"] - phases["create_tables"], 1),
         "data_bytes": data_bytes, "index_bytes": index_bytes, "total_bytes": total_bytes,
         "parquet_bytes": parquet_bytes},
    )

    print(f"\nPostgreSQL SF{sf}: total load {sw.total:.1f}s  " +
          "  ".join(f"{k}={v:.1f}s" for k, v in phases.items()))
    print(f"  data {fmt_bytes(data_bytes)} + indexes {fmt_bytes(index_bytes)} = {fmt_bytes(total_bytes)}"
          f"  ({total_bytes / parquet_bytes:.1f}x the zstd Parquet source)")
    print(f"  -> {path.relative_to(path.parents[1])}")


if __name__ == "__main__":
    main()
