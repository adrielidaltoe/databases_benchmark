"""Generate TPC-H data with DuckDB's tpch extension and write one Parquet file
per table to data/sf<N>/.

    python scripts/gen_tpch.py --sf 1
"""
import argparse
import time

import duckdb

from common import DATA_DIR, SCALE_FACTORS, TABLES, fmt_bytes, parquet_path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sf", type=int, choices=SCALE_FACTORS, required=True)
    sf = ap.parse_args().sf

    out_dir = DATA_DIR / f"sf{sf}"
    out_dir.mkdir(parents=True, exist_ok=True)

    # File-backed (not in-memory) so DuckDB can spill to disk at larger SFs
    db_file = out_dir / "_gen.duckdb"
    for f in (db_file, db_file.with_suffix(".duckdb.wal")):
        f.unlink(missing_ok=True)

    con = duckdb.connect(str(db_file))
    # Leave room for the OS and Docker on a 7 GB laptop
    con.execute("SET memory_limit = '2GB'")
    con.execute("SET threads = 3")
    con.execute("INSTALL tpch")
    con.execute("LOAD tpch")

    t0 = time.perf_counter()
    con.execute(f"CALL dbgen(sf = {sf})")
    print(f"dbgen sf={sf}: {time.perf_counter() - t0:.1f}s")

    for table in TABLES:
        path = parquet_path(sf, table)
        t0 = time.perf_counter()
        con.execute(f"COPY {table} TO '{path}' (FORMAT parquet, COMPRESSION zstd)")
        rows = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        print(f"  {table:<10} {rows:>12,} rows  {fmt_bytes(path.stat().st_size):>10}  "
              f"{time.perf_counter() - t0:.1f}s")

    con.close()
    for f in (db_file, db_file.with_suffix(".duckdb.wal")):
        f.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
