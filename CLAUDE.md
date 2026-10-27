# Row-store vs Columnar Benchmark

## Goal
Database architecture migration analysis for a (fictional) multinational financial
services company: row-based warehouse with 6-8h regulatory report queries, ~200 GB,
$50B daily transactions across 40 countries. Evaluate a claimed 10x performance /
60% cost reduction from migrating to columnar. Deliverables: quantitative benchmark,
TCO comparison, risk assessment, migration plan with rollback procedures.

## Engines
- **PostgreSQL 17** (row-store baseline) - container `bench_pg`, profile `pg`, port 5432
- **ClickHouse 25.8** (columnar target) - container `bench_ch`, profile `ch`, ports 8123/9000
- Credentials for both: user `bench`, password `bench`, database `bench`
- DuckDB is used only for data generation (TPC-H `tpch` extension), not benchmarked

## Hardware constraints (IMPORTANT)
- Laptop: i5-6200U (2 cores / 4 threads), 7.1 GB RAM, SATA SSD
- **Never run both containers at the same time.** Stop one before starting the other.
- Both containers limited to 3 CPUs / 4 GB RAM - keep limits identical for fairness
- Dataset scale: TPC-H SF 1, 3, 10 only. Extrapolate trends; do not attempt larger scales.
- Avoid heavy JVM tools locally (no local Spark). PySpark only with local[3] and 3g driver memory.

## Commands
- Start: `docker compose --profile pg up -d` / `docker compose --profile ch up -d`
- Stop (keeps data): `docker compose --profile pg down`
- **`down -v` DELETES DATA VOLUMES - ask before running it.**
- Postgres shell: `docker exec -it bench_pg psql -U bench -d bench`
- ClickHouse shell: `docker exec -it bench_ch clickhouse-client --user bench --password bench`
- Cold cache between runs: `sync; echo 3 | sudo tee /proc/sys/vm/drop_caches` + restart container

## Known gotchas (already solved - don't reintroduce)
- ClickHouse: never mount host folders inside /var/lib/clickhouse (entrypoint chown fails on read-only mounts)
- ClickHouse: mount individual XML files into config.d / users.d, never the whole folders
  (entrypoint writes users.d/default-user.xml to create the `bench` user)
- Source data lives in ./data, mounted read-only at /data in both containers
- ClickHouse file() only reads from user_files; load via clickhouse-client pipe or clickhouse-connect

## Benchmark methodology
- Tune both engines fairly; document every setting in results
- Postgres: PKs, indexes on join/filter columns, ANALYZE after load
- ClickHouse: MergeTree with sensible ORDER BY keys
- Measure: load time, storage size (compression), cold vs warm query time,
  median + p95 over repeated runs, concurrency, and UPDATE/DELETE workloads
  (late-arriving financial corrections - a known columnar weakness)
- Timeouts: 30 min on both engines; record timeouts as results, not failures

## Code style
- Python preferred; user is an analytics engineer fluent in SQL, Spark SQL and PySpark
- Python clients: psycopg (Postgres), clickhouse-connect (ClickHouse)
