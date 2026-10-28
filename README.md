# Row-store vs Columnar Benchmark

A benchmark of PostgreSQL (row-store) against ClickHouse (columnar) on TPC-H data. It tests a
vendor claim that moving a financial-reporting warehouse to a columnar engine makes it
**10x faster at 60% lower cost**.

## 1. Problem and data

### The problem

A (fictional) multinational financial services company runs its regulatory reporting on a
row-based data warehouse:

- about **200 GB** of data, covering **$50B in daily transactions** across **40 countries**
- regulatory report queries that take **6–8 hours** to run

A vendor proposes migrating to a columnar database, claiming a **10x performance improvement**
and a **60% cost reduction**. The project aims to test that claim with evidence rather than
accept it. The planned deliverables are:

1. **A quantitative benchmark**: load time, storage, cold and warm query time, resource use,
   concurrency, and late-arriving corrections (UPDATE/DELETE).
2. **A TCO comparison**: total cost of ownership for each option.
3. **A risk assessment**: what breaks or gets harder after a migration.
4. **A migration plan** with rollback procedures.

This README covers the parts done so far: the setup, data loading, and query benchmarks at
three scale factors. Concurrency, UPDATE/DELETE, TCO, risk and the migration plan come next.

### Synthetic data: TPC-H

The data is [TPC-H](https://www.tpc.org/tpch/) (Revision 3.0.1), the industry-standard
decision-support benchmark. The specification describes it as *"a suite of business oriented
ad-hoc queries and concurrent data modifications"* that *"examine large volumes of data,
execute queries with a high degree of complexity, and give answers to critical business
questions"*. That matches the regulatory-reporting workload: large scans, multi-table joins
and aggregations over transaction history.

The schema models a wholesale supplier: customers place orders, each order has line items, and
the parts on those line items come from suppliers in nations grouped into regions. `lineitem`
(the transaction-level fact table) and `orders` hold about 7 years of history, 1992–1998.

Data is generated with DuckDB's `tpch` extension (`dbgen`) by
[scripts/gen_tpch.py](scripts/gen_tpch.py) and stored as zstd Parquet in `data/sf<N>/`.
The scale factor (SF) sets the size: SF 1 is about 1 GB of raw data.

| Table | Description | Rows per SF | SF 1 | SF 3 | SF 10 |
|---|---|---|--:|--:|--:|
| `region` | Geographic regions (Europe, Asia, ...) | 5 (fixed) | 5 | 5 | 5 |
| `nation` | Countries, each in one region | 25 (fixed) | 25 | 25 | 25 |
| `supplier` | Suppliers, with their nation and account balance | SF × 10,000 | 10,000 | 30,000 | 100,000 |
| `customer` | Customers, with their nation, market segment and account balance | SF × 150,000 | 150,000 | 450,000 | 1,500,000 |
| `part` | Product catalog | SF × 200,000 | 200,000 | 600,000 | 2,000,000 |
| `partsupp` | Which supplier sells which product, at what cost and available quantity | SF × 800,000 | 800,000 | 2,400,000 | 8,000,000 |
| `orders` | Order header: customer, date, status, priority and total price | SF × 1,500,000 | 1,500,000 | 4,500,000 | 15,000,000 |
| `lineitem` | Main fact table: each item of each order (quantity, price, discount, ship/commit/receipt dates) | ≈ SF × 6,000,000 | 6,001,215 | 17,996,609 | 59,986,052 |
| **Parquet size** | | | 0.23 GB | 0.69 GB | 2.35 GB |

The production system is about 200 GB, roughly SF 200. The laptop can't hold that, so the
benchmark runs SF 1, 3 and 10 and treats the **trend across scales** as the evidence. SF 10 is
where the data first stops fitting in memory, as it would at production scale.

## 2. Setup

### Engines

| | Row-store baseline | Columnar target |
|---|---|---|
| Engine | **PostgreSQL 17.11** | **ClickHouse 25.8.33** |
| Container | `bench_pg` (profile `pg`, port 5432) | `bench_ch` (profile `ch`, ports 8123/9000) |
| Storage | Heap tables + B-tree indexes | MergeTree: compressed column files, sparse primary index |

**Why these two engines:**

- **PostgreSQL** stands in for the current row-based warehouse. It is the most widely used
  open-source row-store, it has a mature cost-based optimizer, and it can be tuned for
  analytics: parallel query, indexes, statistics. That makes it a fair baseline, not a
  deliberately weak one.
- **ClickHouse** stands in for the proposed columnar target. It is a leading open-source
  columnar OLAP engine with vectorized execution, per-column compression and sparse indexes.
  These are the features that the vendor's 10x claim relies on. Its known weaknesses, row-level
  UPDATE/DELETE and large joins, are also typical of columnar engines, which matters for the
  risk assessment.
- Both are open source and run in Docker with the same limits, so the comparison is repeatable
  without licence costs. DuckDB is used only to generate data and is not benchmarked.

**Schemas and tuning.** Both engines are tuned the way a competent team would tune them, and
every setting is saved with each result file.

- **PostgreSQL** ([schema/postgres_tables.sql](schema/postgres_tables.sql),
  [schema/postgres_indexes.sql](schema/postgres_indexes.sql)):
  - primary keys, indexes on all join columns and on the date filter columns, and
    `VACUUM ANALYZE` after load
  - `shared_buffers=1GB`, `effective_cache_size=3GB`, `work_mem=64MB`,
    `max_parallel_workers_per_gather=2`, `max_parallel_workers=3`, `random_page_cost=1.1`
    (SSD), `statement_timeout=30min`
- **ClickHouse** ([schema/clickhouse_tables.sql](schema/clickhouse_tables.sql),
  [clickhouse/](clickhouse/)):
  - MergeTree tables ordered by their keys; `orders` and `lineitem` are partitioned by year and
    sorted by date first, so date filters can skip data
  - `LowCardinality` columns for low-cardinality strings
  - `max_threads=3`, `max_memory_usage=3 GB` per query, `max_server_memory_usage=3.5 GB`,
    external GROUP BY and sort above 1.5 GB, `max_execution_time=1800 s`, query cache off
  - The **join algorithm is left at the default** (in-memory hash join). This matters for the
    SF 10 results below.

### Hardware constraints

Everything runs on one laptop, which shapes the method:

| Resource | Value |
|---|---|
| CPU | Intel Core i5-6200U, 2 cores / 4 threads |
| RAM | 7.1 GB |
| Disk | SATA SSD |
| OS / Docker | Linux 7.0, Docker 29.1 (cgroup v2) |
| **Per-engine limit** | **3 CPUs, 4 GB RAM** (identical for both, set in [docker-compose.yml](docker-compose.yml)) |

- **Only one engine runs at a time.** The other container is stopped, so they never compete
  for CPU, memory, disk or page cache.
- **Scale is capped at SF 10.** Larger scales don't fit on the machine. Results for SF 200 are
  extrapolated from the SF 1 → 3 → 10 trend, not measured.
- **The 4 GB limit includes the page cache.** At SF 1 and SF 3 both engines' data fits in
  memory, so warm runs never read from disk. At SF 10, Postgres's 16.7 GB doesn't fit, while
  ClickHouse's 4.0 GB just about does. That's the same situation the production 200 GB would
  be in, at a larger ratio.
- The absolute times are laptop times. The **ratios** between the engines, and how they change
  with scale, are the useful result.

## 3. Queries

The benchmark runs the 22 standard TPC-H queries with the specification's validation
parameters, taken from DuckDB's `tpch` extension. The query text is the same on both engines,
in [queries/postgres/](queries/postgres/) and [queries/clickhouse/](queries/clickhouse/). The
only difference is that ClickHouse's Q13 adds `SETTINGS join_use_nulls = 1`, so the LEFT JOIN
follows SQL semantics. The business names come from the TPC-H specification, Revision 3.0.1,
§2.4.

| ID | Business name | Workload pattern |
|---|---|---|
| Q1 | Pricing Summary Report | Full scan of `lineitem` with a date filter; 8 aggregates grouped by 2 low-cardinality columns |
| Q2 | Minimum Cost Supplier | 5-table join; **correlated subquery** (minimum cost per part); top 100 |
| Q3 | Shipping Priority | 3-table join with date filters; group by order; top 10 by revenue |
| Q4 | Order Priority Checking | Date range on `orders` + **correlated `EXISTS`** on `lineitem` (semi-join); group by priority |
| Q5 | Local Supplier Volume | 6-table join where customer and supplier are in the same nation; group by nation |
| Q6 | Forecasting Revenue Change | Single-table scan with selective range filters (date, discount, quantity); one sum |
| Q7 | Volume Shipping | 6-table join, `nation` joined twice, OR filter on a nation pair; group by nations and year |
| Q8 | National Market Share | 8-table join; ratio of two conditional sums (`CASE` inside `sum`); group by year |
| Q9 | Product Type Profit Measure | 6-table join with a `LIKE` filter on part names; group by nation and year. The heaviest join |
| Q10 | Returned Item Reporting | 4-table join; group by customer (7 columns); top 20 |
| Q11 | Important Stock Identification | 3-table join; `HAVING` compared with an uncorrelated total (threshold report) |
| Q12 | Shipping Modes and Order Priority | 2-table join; `IN` list and date-column comparisons; conditional counts |
| Q13 | Customer Distribution | `LEFT OUTER JOIN` + `NOT LIKE`; aggregation of an aggregation |
| Q14 | Promotion Effect | 2-table join over one month; conditional ratio (percentage) |
| Q15 | Top Supplier | CTE aggregate compared with its own `max` (top-1 with ties) |
| Q16 | Parts/Supplier Relationship | 2-table join; `count(DISTINCT)`; `NOT IN` subquery; `NOT LIKE` |
| Q17 | Small-Quantity-Order Revenue | 2-table join; **correlated subquery** (average quantity per part) |
| Q18 | Large Volume Customer | `IN` subquery with `GROUP BY … HAVING` over all of `lineitem`; 3-table join; top 100 |
| Q19 | Discounted Revenue | 2-table join with a large OR of three AND groups (brand, container, size, quantity) |
| Q20 | Potential Part Promotion | Nested `IN` subqueries + **correlated sum** per (part, supplier) |
| Q21 | Suppliers Who Kept Orders Waiting | 4-table join + **correlated `EXISTS` and `NOT EXISTS`** on `lineitem` |
| Q22 | Global Sales Opportunity | Substring filter; uncorrelated average; **correlated `NOT EXISTS`** (anti-join) |

### CTE rewrites of the correlated queries

Six queries use correlated subqueries: Q2, Q4, Q17, Q20, Q21 and Q22. ClickHouse supports
these only as a Beta feature. Each one also exists as a hand-written rewrite using a `WITH`
clause: the correlated part is computed once for every key and joined back. The rewrites are in
[queries/postgres/rewritten/](queries/postgres/rewritten/) and
[queries/clickhouse/rewritten/](queries/clickhouse/rewritten/), with the same text on both
engines; ClickHouse's Q22 adds `join_use_nulls = 1`. Both versions are timed:

- **original**: what happens if existing reports are migrated unchanged
- **rewritten**: what each engine can do once the SQL has been adapted

At SF 1, [scripts/check_rewrites.py](scripts/check_rewrites.py) confirmed that every rewrite
returns exactly the same result as its original, on both engines.

## 4. Results

### How the queries are measured

[scripts/run_queries.py](scripts/run_queries.py) runs every query and version on one engine:

- **Cold run**: the engine is stopped, its data files are evicted from the OS page cache, and
  it is restarted, so the query starts with empty engine buffers and an empty OS cache. The
  eviction uses `posix_fadvise(DONTNEED)` on every file in the data volume, from an
  unprivileged throwaway container, so no root access is needed.
- **Warm runs**: 5 more runs straight after. The median and p95 are reported. With 5 runs, the
  p95 is close to the slowest run.
- **Timeout**: 30 minutes on both engines. A timeout or a memory-limit error is recorded as
  the result, and the remaining runs of that query are skipped.
- **Time** is client-side wall-clock time, including fetching the result (which is small for
  every query).
- **Resources** come from the container's cgroup v2 files, measured the same way for both
  engines: CPU time, average busy cores, peak memory (total and working memory) and bytes read
  from or written to disk.
- **Scan / output**: rows and bytes read, measured once per query. ClickHouse reports them in
  `system.query_log`; for Postgres they are the change in its table and index statistics
  counters. They are compared with the number of rows returned.
- **Correctness**: `--compare` checks that both engines return the same rows (numbers within
  1e-6 relative). Every completed query matches, except Q8: ClickHouse returns
  `mkt_share = 0.0344` where Postgres returns `0.0344358…`. ClickHouse `Decimal` division keeps
  the dividend's scale, so the result is truncated to 4 digits. That's within TPC-H tolerance,
  but it would be a **silent precision loss in financial ratios** after a migration. The query
  is kept as written, and the finding is documented in
  [queries/clickhouse/q08.sql](queries/clickhouse/q08.sql).

Raw results are in `results/`: `load_*.json` and `loads.csv` for loading, `queries_*.json`
(every run, with the engine settings) and `queries.csv` for queries.

### Data load

[scripts/load_postgres.py](scripts/load_postgres.py) and
[scripts/load_clickhouse.py](scripts/load_clickhouse.py) stream the Parquet files in batches
of 500,000 rows. The batch size is the same for both engines, so the client-side work is the
same.

| | SF 1 | SF 3 | SF 10 |
|---|--:|--:|--:|
| **PostgreSQL total load** | **152 s** | **420 s** | **1,226 s** |
| ↳ COPY | 81.7 s | 195.6 s | 571.6 s |
| ↳ primary keys + indexes | 52.3 s | 170.8 s | 517.2 s |
| ↳ VACUUM ANALYZE | 14.3 s | 51.3 s | 134.2 s |
| ↳ CHECKPOINT | 3.8 s | 2.3 s | 2.6 s |
| **ClickHouse total load** | **27 s** | **74 s** | **262 s** |
| ↳ INSERT | 22.9 s | 71.0 s | 258.2 s |
| ↳ background merges settle | 4.2 s | 3.2 s | 3.3 s |
| **ClickHouse faster by** | **5.6x** | **5.7x** | **4.7x** |
| PostgreSQL on disk (data + indexes) | 1.68 GB (1.25 + 0.43) | 5.02 GB (3.75 + 1.27) | 16.73 GB (12.49 + 4.24) |
| ClickHouse on disk (compressed) | 0.39 GB | 1.19 GB | 4.01 GB |
| ClickHouse uncompressed | 0.81 GB | 2.42 GB | 8.14 GB |
| **ClickHouse smaller by** | **4.3x** | **4.2x** | **4.2x** |
| Source Parquet (zstd) | 0.23 GB | 0.69 GB | 2.35 GB |

- ClickHouse loads about 5x faster. Its INSERT alone is faster than Postgres's COPY, and it
  builds no secondary indexes.
- Indexes account for 34–42% of Postgres's load time and 25% of its storage. That's the
  cost of the indexes that make its selective queries fast (see Q17 and Q19 below).
- ClickHouse uses about 4.2x less storage at every scale: no indexes, plus about 2x
  compression on top of columnar storage.

### Query results: summary

The speedup is Postgres time ÷ ClickHouse time; above 1 means ClickHouse is faster.
"As written" is the geometric mean over the original queries that completed on both engines.
"Best version" uses each engine's faster version of each query (original or rewrite).

| | SF 1 | SF 3 | SF 10 |
|---|--:|--:|--:|
| Original queries completed on both engines | 21 of 22 | 21 of 22 | **17 of 22** |
| **Cold speedup, as written** (geometric mean) | **15.4x** | **19.0x** | **29.0x** |
| **Warm speedup, as written** (geometric mean) | **2.5x** | **11.2x** | **23.7x** |
| Total cold time, best version (PG vs CH) | 156 s vs 10 s | 611 s vs 34 s | 1,371 s vs 69 s ¹ |
| Total warm time, best version (PG vs CH) | 21 s vs 6 s | 437 s vs 21 s | 1,111 s vs 48 s ¹ |
| Disk read, cold runs (PG vs CH) ² | 13.3 vs 1.0 GiB | 50.0 vs 3.1 GiB | 113.7 vs 5.7 GiB |
| Median busy cores, warm (of 3; PG vs CH) ² | 2.6 vs 2.7 | 1.3 vs 2.9 | **1.0 vs 2.9** |
| CPU time, warm (PG vs CH) ² | 52 vs 16 s | 359 vs 56 s | 979 vs 92 s |
| Queries where ClickHouse is slower when warm | Q5, Q8, Q17, Q19 | Q17, Q19 | Q17, Q19 |
| **Failures** | CH: Q21 orig. (memory) | CH: Q21 orig. (memory); PG: Q4 rewrite (timeout) | **CH: Q3, Q5, Q8, Q9, Q21 orig. (memory)**; PG: Q4 rewrite (timeout) |

¹ At SF 10, totals cover only the 18 queries where both engines completed at least one version.
The geometric means exclude failed queries, which flatters ClickHouse at SF 10.
² Summed or median over the original queries that completed on both engines.

### Key findings

1. **The speedup grows with scale and passes 10x at SF 3.** At SF 1 everything is in memory,
   and ClickHouse is only about 2.5x faster warm. Its advantage there is purely CPU efficiency:
   vectorized execution, and it reads only the columns a query uses. By SF 10 Postgres's data
   no longer fits in memory. Postgres averages about **1 busy core out of 3** (most of its time
   is spent waiting for the disk), while ClickHouse keeps all 3 busy and reads about **20x
   less data**. The production system (~200 GB, a 6–8 h reporting window) is far past this
   point, so for queries that complete, the trend **supports the 10x claim**.
2. **The win comes from bytes, not rows.** ClickHouse often reads *more* rows than Postgres,
   because it has no row-level indexes and scans whole column ranges. But it reads far fewer
   bytes: only the columns a query needs, compressed. On a cold Q6 at SF 1, ClickHouse read
   9 MiB from disk where Postgres read 781 MiB.
3. **ClickHouse fails join-heavy queries at SF 10 with the default join.** Q3, Q5, Q8 and Q9
   hit the 3 GB per-query memory limit after 4–5 s, having spilled **0 MB** to disk. The
   default ClickHouse join builds its whole hash table in memory, whereas Postgres spills joins
   to disk automatically and completed every original query. At production scale these
   reports would fail outright rather than run slowly, unless memory or the join algorithm is
   changed. This is the **most important risk finding so far**.
4. **Correlated subqueries are a migration cost.** ClickHouse's Q21 runs out of memory at every
   scale as written. Its CTE rewrite works (19 s at SF 10) and matches Postgres's results. The
   rewrites help ClickHouse (Q17: 3.3 s → 1.6 s at SF 10) and often hurt Postgres, which already
   runs the correlated forms well using its indexes (Q21: 317 s → 1,358 s at SF 10). Migrated
   reports need rewriting and re-validation; they can't simply be moved over.
5. **Postgres indexes still win on very selective queries.** Q17 and Q19 stay faster on
   Postgres when warm at every scale, because its indexes read 29–58x fewer rows.
6. **Open question: the Postgres Q4 rewrite is unstable.** It timed out on its cold run at
   SF 3, and at SF 10 the cold run finished in 108 s but the first warm run then hit 30 minutes.
   It doesn't affect the comparison, because the original Q4 is Postgres's faster version, but
   it hasn't been investigated.

**Memory note:** "working memory" (`peak_anon_mb` in the JSON) covers the whole container.
ClickHouse's server process alone accounts for about 250–300 MB of it, even for tiny queries.
Postgres stayed below 1 GB on every query. ClickHouse reached 2–3 GB on large joins, and up to
the 3 GB limit on the queries that failed.

### Query results per scale factor

Times are in seconds. **OOM** means the 3 GB per-query memory limit was exceeded; *timeout*
means 30 minutes. "Disk read" is MiB read from disk during the cold run. "Rows read" includes
index entries on Postgres, and comes from the first run of each query. † marks a run that
failed, so its rows-read count is only what was read before it failed.

#### SF 1

| Query | Version | PG cold | PG warm median (p95) | CH cold | CH warm median (p95) | Speedup cold | Speedup warm | PG disk read cold (MB) | CH disk read cold (MB) | PG rows read | CH rows read |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| Q01 | original | 5.40 | 3.83 (3.94) | 0.40 | 0.36 (0.43) | 13.7x | 10.5x | 853 | 79.3 | 6,001,216 | 5,920,460 |
| Q02 | original | 3.65 | 0.23 (0.30) | 0.27 | 0.09 (0.10) | 13.4x | 2.4x | 186 | 9.70 | 381,516 | 1,820,060 |
| Q02 | rewritten | 4.69 | 0.69 (0.70) | 0.31 | 0.18 (0.18) | 15.2x | 3.8x | 178 | 9.20 | 278,409 | 1,820,060 |
| Q03 | original | 11.4 | 0.72 (0.73) | 0.59 | 0.46 (0.48) | 19.1x | 1.6x | 1,174 | 46.1 | 1,042,474 | 4,126,946 |
| Q04 | original | 6.97 | 0.17 (0.18) | 0.32 | 0.16 (0.17) | 22.1x | 1.1x | 900 | 46.2 | 140,303 | 6,066,751 |
| Q04 | rewritten | 4.78 | 2.02 (2.03) | 0.47 | 0.30 (0.32) | 10.2x | 6.8x | 997 | 46.1 | 6,058,433 | 6,066,751 |
| Q05 | original | 7.99 | 0.33 (0.36) | 0.92 | 0.75 (0.76) | 8.7x | 0.4x | 778 | 93.7 | 665,930 | 6,388,842 |
| Q06 | original | 12.1 | 0.39 (0.40) | 0.05 | 0.03 (0.03) | 246.5x | 11.7x | 781 | 9.30 | 909,455 | 909,455 |
| Q07 | original | 25.3 | 0.76 (0.81) | 0.61 | 0.46 (0.47) | 41.5x | 1.6x | 1,051 | 42.9 | 1,709,453 | 3,488,500 |
| Q08 | original | 6.46 | 0.18 (0.20) | 1.06 | 0.79 (0.94) | 6.1x | 0.2x | 600 | 120 | 380,032 | 6,818,533 |
| Q09 | original | 12.0 | 1.47 (1.52) | 1.49 | 1.04 (1.06) | 8.0x | 1.4x | 1,257 | 152 | 1,200,973 | 8,511,240 |
| Q10 | original | 8.25 | 0.44 (0.47) | 0.43 | 0.25 (0.26) | 19.4x | 1.8x | 953 | 50.8 | 435,926 | 6,214,045 |
| Q11 | original | 3.05 | 0.08 (0.09) | 0.15 | 0.07 (0.08) | 20.4x | 1.2x | 141 | 9.70 | 64,214 | 1,620,050 |
| Q12 | original | 4.13 | 0.75 (0.76) | 0.29 | 0.07 (0.09) | 14.4x | 11.4x | 1,022 | 23.3 | 6,032,207 | 4,074,528 |
| Q13 | original | 1.68 | 1.58 (1.62) | 0.76 | 0.29 (0.30) | 2.2x | 5.4x | 206 | 43.2 | 1,650,004 | 1,650,000 |
| Q14 | original | 4.71 | 0.17 (0.18) | 0.07 | 0.03 (0.03) | 62.9x | 5.8x | 348 | 2.80 | 275,987 | 282,073 |
| Q15 | original | 3.42 | 0.26 (0.29) | 0.07 | 0.03 (0.03) | 52.6x | 9.7x | 795 | 4.20 | 225,955 | 469,130 |
| Q16 | original | 0.81 | 0.62 (0.68) | 0.13 | 0.07 (0.08) | 6.2x | 8.4x | 48.3 | 6.00 | 1,030,004 | 1,010,000 |
| Q17 | original | 5.81 | 0.88 (0.89) | 0.47 | 0.31 (0.31) | 12.4x | 2.8x | 825 | 69.9 | 6,388,257 | 12,202,430 |
| Q17 | rewritten | 0.83 | 0.06 (0.06) | 0.34 | 0.12 (0.13) | 2.5x | 0.5x | 77.6 | 69.9 | 212,180 | 12,202,430 |
| Q18 | original | 9.88 | 7.50 (8.35) | 0.57 | 0.34 (0.34) | 17.3x | 22.0x | 1,026 | 62.6 | 19,654,052 | 13,652,430 |
| Q19 | original | 1.59 | 0.05 (0.05) | 0.53 | 0.17 (0.17) | 3.0x | 0.3x | 139 | 94.4 | 214,305 | 6,201,215 |
| Q20 | original | 15.8 | 0.10 (0.10) | 0.18 | 0.06 (0.06) | 86.5x | 1.8x | 538 | 19.3 | 272,787 | 1,919,480 |
| Q20 | rewritten | 5.83 | 0.12 (0.13) | 0.14 | 0.05 (0.05) | 40.2x | 2.5x | 522 | 19.5 | 278,475 | 1,919,480 |
| Q21 | original | 22.6 | 0.71 (0.74) | **OOM** | **OOM** | - | - | 1,178 | - | 2,388,736 | 29,412,930 † |
| Q21 | rewritten | 22.1 | 5.75 (6.23) | 1.14 | 0.71 (0.87) | 19.4x | 8.1x | 1,212 | 71.4 | 6,587,825 | 13,512,455 |
| Q22 | original | 0.30 | 0.10 (0.10) | 0.17 | 0.07 (0.10) | 1.7x | 1.3x | 40.5 | 17.7 | 312,620 | 1,800,000 |
| Q22 | rewritten | 0.37 | 0.21 (0.23) | 0.10 | 0.06 (0.07) | 3.7x | 3.4x | 40.4 | 10.6 | 1,650,000 | 1,800,000 |

#### SF 3

| Query | Version | PG cold | PG warm median (p95) | CH cold | CH warm median (p95) | Speedup cold | Speedup warm | PG disk read cold (MB) | CH disk read cold (MB) | PG rows read | CH rows read |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| Q01 | original | 12.8 | 10.5 (10.8) | 1.06 | 0.80 (0.85) | 12.0x | 13.1x | 2,446 | 246 | 17,996,610 | 17,756,038 |
| Q02 | original | 11.1 | 0.67 (0.67) | 0.44 | 0.23 (0.25) | 25.3x | 2.8x | 546 | 31.3 | 1,148,775 | 5,460,060 |
| Q02 | rewritten | 12.4 | 2.12 (2.12) | 0.63 | 0.46 (0.47) | 19.8x | 4.6x | 532 | 30.6 | 837,977 | 5,460,060 |
| Q03 | original | 15.1 | 6.05 (6.22) | 1.86 | 1.52 (1.85) | 8.2x | 4.0x | 3,108 | 152 | 19,352,978 | 12,370,393 |
| Q04 | original | 22.6 | 17.9 (18.8) | 1.11 | 0.45 (0.54) | 20.4x | 39.9x | 2,689 | 158 | 423,588 | 18,185,025 |
| Q04 | rewritten | timeout | timeout | 1.28 | 0.77 (0.77) | - | - | - | 158 | 18,169,322 † | 18,185,025 |
| Q05 | original | 24.7 | 13.0 (13.1) | 3.12 | 2.97 (3.09) | 7.9x | 4.4x | 2,464 | 303 | 1,992,142 | 19,160,068 |
| Q06 | original | 32.5 | 22.7 (22.9) | 0.20 | 0.06 (0.07) | 164.3x | 354.2x | 2,345 | 32.6 | 2,733,944 | 2,733,944 |
| Q07 | original | 162 | 119 (127) | 1.97 | 1.64 (1.96) | 82.2x | 72.5x | 6,833 | 137 | 5,160,620 | 10,444,237 |
| Q08 | original | 18.9 | 8.17 (12.6) | 3.24 | 2.77 (2.92) | 5.8x | 3.0x | 1,769 | 381 | 896,820 | 20,442,850 |
| Q09 | original | 90.0 | 86.0 (88.3) | 7.54 | 4.04 (4.50) | 11.9x | 21.3x | 9,487 | 481 | 3,670,508 | 25,526,634 |
| Q10 | original | 17.1 | 6.01 (6.12) | 1.15 | 0.69 (0.79) | 14.9x | 8.7x | 3,051 | 169 | 18,618,708 | 18,629,980 |
| Q11 | original | 9.69 | 0.26 (0.26) | 0.29 | 0.15 (0.15) | 33.4x | 1.7x | 426 | 29.8 | 193,814 | 4,860,050 |
| Q12 | original | 11.4 | 5.44 (5.97) | 0.85 | 0.18 (0.21) | 13.4x | 30.1x | 3,079 | 68.6 | 18,089,757 | 12,240,500 |
| Q13 | original | 4.98 | 4.52 (4.54) | 2.37 | 0.71 (0.74) | 2.1x | 6.4x | 618 | 127 | 4,950,004 | 4,950,000 |
| Q14 | original | 13.8 | 0.82 (0.83) | 0.15 | 0.07 (0.07) | 91.1x | 12.2x | 1,034 | 10.6 | 824,641 | 853,952 |
| Q15 | original | 43.1 | 45.0 (46.6) | 0.14 | 0.06 (0.07) | 312.2x | 737.0x | 1,882 | 12.4 | 678,083 | 1,422,640 |
| Q16 | original | 1.99 | 1.31 (1.33) | 0.27 | 0.13 (0.14) | 7.4x | 10.0x | 144 | 20.8 | 3,090,004 | 3,030,000 |
| Q17 | original | 17.5 | 5.66 (7.11) | 1.31 | 0.86 (0.87) | 13.3x | 6.6x | 2,489 | 227 | 19,158,796 | 36,593,218 |
| Q17 | rewritten | 2.55 | 0.11 (0.12) | 1.00 | 0.36 (0.47) | 2.6x | 0.3x | 230 | 235 | 635,978 | 36,593,218 |
| Q18 | original | 55.2 | 63.1 (67.2) | 1.59 | 1.05 (1.14) | 34.7x | 59.8x | 4,618 | 217 | 58,923,517 | 40,943,218 |
| Q19 | original | 5.33 | 0.15 (0.15) | 1.31 | 0.44 (0.58) | 4.1x | 0.3x | 428 | 305 | 644,270 | 18,596,609 |
| Q20 | original | 60.0 | 0.75 (0.77) | 0.35 | 0.10 (0.10) | 171.5x | 7.5x | 1,606 | 64.7 | 821,395 | 5,763,969 |
| Q20 | rewritten | 18.4 | 0.57 (0.58) | 0.40 | 0.15 (0.17) | 45.5x | 3.8x | 1,566 | 63.8 | 838,289 | 5,763,969 |
| Q21 | original | 37.4 | 25.3 (25.8) | **OOM** | **OOM** | - | - | 4,772 | - | 5,706,663 | 38,828,794 † |
| Q21 | rewritten | 174 | 197 (202) | 3.85 | 2.67 (3.24) | 45.3x | 73.9x | 7,077 | 241 | 19,759,210 | 40,523,243 |
| Q22 | original | 0.73 | 0.25 (0.27) | 0.36 | 0.23 (0.26) | 2.0x | 1.1x | 120 | 48.2 | 937,841 | 5,400,000 |
| Q22 | rewritten | 1.11 | 0.72 (0.77) | 0.23 | 0.15 (0.17) | 4.7x | 4.7x | 120 | 26.5 | 4,950,000 | 5,400,000 |

#### SF 10

| Query | Version | PG cold | PG warm median (p95) | CH cold | CH warm median (p95) | Speedup cold | Speedup warm | PG disk read cold (MB) | CH disk read cold (MB) | PG rows read | CH rows read |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| Q01 | original | 36.9 | 36.6 (37.2) | 2.92 | 2.57 (2.69) | 12.6x | 14.2x | 7,997 | 772 | 59,986,053 | 59,147,358 |
| Q02 | original | 38.7 | 10.4 (13.4) | 1.24 | 0.88 (0.90) | 31.3x | 11.8x | 1,853 | 94.4 | 3,705,311 | 18,200,060 |
| Q02 | rewritten | 44.5 | 13.6 (15.1) | 2.13 | 2.01 (2.13) | 20.9x | 6.8x | 1,771 | 93.7 | 2,805,821 | 18,200,060 |
| Q03 | original | 31.3 | 25.8 (26.0) | **OOM** | **OOM** | - | - | 10,252 | - | 76,486,060 | 37,769,832 † |
| Q04 | original | 91.4 | 72.1 (76.0) | 2.83 | 1.57 (1.75) | 32.3x | 45.8x | 8,995 | 468 | 1,406,498 | 60,575,876 |
| Q04 | rewritten | 108 | timeout | 3.83 | 2.73 (2.92) | 28.2x | - | 10,760 | 468 | 60,559,723 | 60,575,876 |
| Q05 | original | 40.7 | 33.2 (34.7) | **OOM** | **OOM** | - | - | 10,271 | - | 63,862,079 | 27,420,762 † |
| Q06 | original | 89.9 | 78.5 (81.1) | 0.37 | 0.24 (0.27) | 245.6x | 328.6x | 7,824 | 92.9 | 9,099,165 | 9,099,165 |
| Q07 | original | 73.2 | 72.7 (81.3) | 6.48 | 5.95 (6.13) | 11.3x | 12.2x | 10,369 | 431 | 34,830,491 | 34,830,375 |
| Q08 | original | 70.0 | 55.5 (56.0) | **OOM** | **OOM** | - | - | 6,857 | - | 4,407,161 | 31,790,748 † |
| Q09 | original | 507 | 497 (502) | **OOM** | **OOM** | - | - | 49,465 | - | 19,784,944 | 32,733,817 † |
| Q10 | original | 50.0 | 39.3 (39.7) | 6.05 | 5.51 (5.64) | 8.3x | 7.1x | 10,194 | 502 | 62,059,294 | 62,066,971 |
| Q11 | original | 30.0 | 1.31 (1.33) | 1.00 | 0.76 (0.77) | 30.1x | 1.7x | 1,420 | 95.8 | 751,976 | 16,200,050 |
| Q12 | original | 35.3 | 31.5 (31.7) | 1.94 | 0.59 (0.79) | 18.2x | 53.7x | 10,274 | 201 | 60,296,859 | 40,804,316 |
| Q13 | original | 115 | 42.2 (42.3) | 11.6 | 2.59 (2.74) | 9.9x | 16.3x | 2,182 | 426 | 16,500,004 | 16,500,000 |
| Q14 | original | 44.2 | 32.0 (32.4) | 0.26 | 0.16 (0.16) | 168.7x | 203.6x | 3,443 | 18.9 | 2,749,227 | 2,761,856 |
| Q15 | original | 205 | 191 (194) | 0.28 | 0.20 (0.20) | 722.7x | 971.6x | 5,399 | 31.1 | 2,265,715 | 4,638,368 |
| Q16 | original | 5.96 | 4.45 (4.58) | 0.54 | 0.41 (0.42) | 11.0x | 10.8x | 485 | 58.4 | 10,300,004 | 10,100,000 |
| Q17 | original | 48.3 | 19.6 (19.9) | 4.07 | 3.31 (3.70) | 11.9x | 5.9x | 8,539 | 696 | 63,890,269 | 121,972,104 |
| Q17 | rewritten | 8.05 | 0.48 (0.49) | 2.55 | 1.55 (1.87) | 3.2x | 0.3x | 776 | 696 | 2,122,774 | 121,972,104 |
| Q18 | original | 140 | 128 (130) | 4.96 | 4.16 (4.50) | 28.3x | 30.8x | 18,058 | 643 | 121,472,736 | 136,472,104 |
| Q19 | original | 15.4 | 0.69 (0.70) | 3.42 | 1.40 (1.52) | 4.5x | 0.5x | 1,385 | 935 | 2,142,588 | 61,986,052 |
| Q20 | original | 720 | 683 (688) | 0.85 | 0.28 (0.38) | 842.3x | 2396.7x | 17,622 | 199 | 2,369,181 | 19,199,190 |
| Q20 | rewritten | 63.6 | 51.3 (52.6) | 1.03 | 0.42 (0.43) | 61.8x | 122.1x | 5,388 | 199 | 2,793,235 | 19,199,190 |
| Q21 | original | 326 | 317 (319) | **OOM** | **OOM** | - | - | 35,810 | - | 20,655,010 | 49,584,575 † |
| Q21 | rewritten | 1,317 | 1,358 (1,368) | 20.7 | 19.0 (19.8) | 63.6x | 71.4x | 39,232 | 738 | 65,855,712 | 135,072,129 |
| Q22 | original | 2.23 | 0.81 (0.82) | 0.81 | 0.54 (0.56) | 2.7x | 1.5x | 400 | 162 | 3,126,781 | 18,000,000 |
| Q22 | rewritten | 3.78 | 2.87 (2.91) | 0.66 | 0.51 (0.66) | 5.7x | 5.7x | 400 | 88.6 | 16,500,000 | 18,000,000 |

