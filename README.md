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

This README covers the parts done so far: the setup, data loading, query benchmarks at
three scale factors, the ClickHouse join tuning needed to complete them at SF 10, the write
workload (sections 6–8), concurrency (section 9), the TCO comparison (section 10) and the risk
assessment (section 11). The migration plan comes next.

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
  - `memory_worker_correct_memory_tracker=1`, added for the write benchmark, so the server's
    memory count follows the real RSS (see [section 7](#7-write-benchmark))
  - The **join algorithm is left at the default** (in-memory hash join) in the profile. At SF 10
    four queries run out of memory with it; they get a per-query setting instead, as a separate
    "tuned" version (see [section 5](#5-clickhouse-join-tuning)).

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

### Tuned versions (ClickHouse only)

Q3, Q5, Q8 and Q9 also exist as a **tuned** version in
[queries/clickhouse/tuned/](queries/clickhouse/tuned/): the original SQL unchanged, plus
`SETTINGS join_algorithm = 'full_sorting_merge'`. With the default join they run out of memory
on ClickHouse at SF 10. [Section 5](#5-clickhouse-join-tuning) explains how the setting was
chosen. Postgres has no tuned versions; the tuned rows are compared with Postgres's original.

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
  1e-6 relative). ClickHouse tuned versions are checked against Postgres's original. Every
  completed query matches, except Q8 (original and tuned): ClickHouse returns
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
"Best version" uses each engine's fastest version of each query (original, rewrite, or on
ClickHouse the tuned version).

**Why a geometric mean.** Each query gives one speedup, a ratio, and the geometric mean is the
standard way to average ratios:

- **Wins and losses count equally.** A query that is 4x faster and one that is 4x slower
  average to 1.0x. An arithmetic mean would give (4 + 0.25) / 2 = 2.1x, overstating the
  advantage. Our per-query speedups range from 0.24x to over 2,000x, so this matters a lot.
- **Every query counts equally**, whether it takes 0.1 s or 10 minutes.
- **It's the benchmark convention.** TPC-H's own Power metric, and SPEC CPU, use it, which
  makes the results comparable with how vendor claims are usually calculated.

**How it is calculated:** multiply the per-query speedups, then take the n-th root, where n is
the number of queries. For example, with three queries from the SF 1 warm results:

| Query | PG warm | CH warm | Speedup (PG ÷ CH) |
|---|--:|--:|--:|
| Q1 | 3.828 s | 0.312 s | 12.27x |
| Q6 | 0.387 s | 0.030 s | 12.90x |
| Q8 | 0.185 s | 0.760 s | 0.24x (ClickHouse slower) |

1. Multiply the speedups: 12.27 × 12.90 × 0.24 = 38.0
2. Take the cube root, because there are 3 queries: ∛38.0 = **3.36x**

The geometric mean is the single speedup that, applied to every query, gives the same combined
effect (3.36 × 3.36 × 3.36 = 38.0). The arithmetic mean of the same three queries,
(12.27 + 12.90 + 0.24) / 3 = 8.5x, almost ignores the fact that ClickHouse was 4x slower on Q8.

Cold and warm are averaged separately: cold uses each query's single cold run, and warm uses
each query's median over its 5 warm runs. The table below applies this to every original query
that completed on both engines (21 of the 22 at SF 1).

| | SF 1 | SF 3 | SF 10 |
|---|--:|--:|--:|
| Original queries completed on both engines | 21 of 22 | 21 of 22 | **17 of 22** |
| **Cold speedup, as written** (geometric mean) | **15.5x** | **20.2x** | **28.8x** |
| **Warm speedup, as written** (geometric mean) | **2.6x** | **10.6x** | **22.8x** |
| Cold / warm speedup with the tuned Q3, Q5, Q8, Q9 ¹ | not needed | not needed | **19.5x / 15.6x** (21 of 22) |
| Total cold time, best version (PG vs CH) | 156 s vs 10 s | 611 s vs 34 s | 2,020 s vs 168 s |
| Total warm time, best version (PG vs CH) | 21 s vs 6 s | 437 s vs 22 s | 1,722 s vs 152 s |
| Disk read, cold runs (PG vs CH) ² | 13.3 vs 1.0 GiB | 50.0 vs 3.1 GiB | 113.7 vs 5.7 GiB |
| Median busy cores, warm (of 3; PG vs CH) ² | 2.6 vs 2.8 | 1.3 vs 2.9 | **1.0 vs 3.0** |
| CPU time, warm (PG vs CH) ² | 52 vs 16 s | 359 vs 58 s | 979 vs 99 s |
| Queries where ClickHouse is slower when warm (best version) | Q5, Q8, Q17, Q19, Q21 | Q17, Q19 | Q17, Q19 |
| **Failures** | CH: Q21 orig. (memory) | CH: Q21 orig. (memory); PG: Q4 rewrite (timeout) | **CH: Q3, Q5, Q8, Q9, Q21 orig. (memory)**; PG: Q4 rewrite (timeout) |

¹ The as-written geometric means leave out the queries ClickHouse failed, which flatters it at
SF 10. This row puts Q3, Q5, Q8 and Q9 back in, using their tuned versions. These are four of
the heaviest joins, and ClickHouse's slowest queries once tuned, so the SF 10 warm speedup
drops from 22.8x to 15.6x. Only Q21 is still missing: its original fails under every join
setting, and its rewrite is counted in the best-version totals. All 22 queries have a
best version at every scale.
² Summed or median over the original queries that completed on both engines.

### Key findings

1. **The speedup grows with scale and passes 10x at SF 3.** At SF 1 everything is in memory,
   and ClickHouse is only about 2.6x faster warm. Its advantage there is purely CPU efficiency:
   vectorized execution, and it reads only the columns a query uses. By SF 10 Postgres's data
   no longer fits in memory. Postgres averages about **1 busy core out of 3** (most of its time
   is spent waiting for the disk), while ClickHouse keeps all 3 busy and reads about **20x
   less data**. The production system (~200 GB, a 6–8 h reporting window) is far past this
   point, so the trend **supports the 10x claim**: 22.8x warm at SF 10 as written, and still
   15.6x once the four tuned joins are counted. With ReplacingMergeTree, which corrections
   require (sections 7 and 8), the warm speedup drops to 10.9x and 8.6x.
2. **The win comes from bytes, not rows.** ClickHouse often reads *more* rows than Postgres,
   because it has no row-level indexes and scans whole column ranges. But it reads far fewer
   bytes: only the columns a query needs, compressed. On a cold Q6 at SF 1, ClickHouse read
   9 MiB from disk where Postgres read 781 MiB.
3. **ClickHouse fails join-heavy queries at SF 10 with the default join.** Q3, Q5, Q8 and Q9
   hit the 3 GB per-query memory limit after 4–6 s, having spilled **0 MB** to disk. The
   default ClickHouse join builds its whole hash table in memory, whereas Postgres spills joins
   to disk automatically and completed every original query. A per-query join setting fixes
   all four (see [section 5](#5-clickhouse-join-tuning)), but they become ClickHouse's slowest
   queries: Q5 is only **1.3x** faster than Postgres when warm, and Q8 2.0x. No single
   server-wide setting runs all 22 reports on this memory budget. At production scale, every
   large report would need to be found and tuned one by one, or the servers would need more
   memory per query. This is the **most important risk finding so far**.
4. **Correlated subqueries are a migration cost.** ClickHouse's Q21 runs out of memory at every
   scale as written, under every join setting tried. Its CTE rewrite works (20 s at SF 10) and
   matches Postgres's results. The rewrites help ClickHouse (Q17: 3.6 s → 1.5 s at SF 10) and often hurt Postgres, which already
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
means 30 minutes. **Tuned** rows are ClickHouse-only: their Postgres columns repeat Postgres's
original, which the speedups are computed against. "Disk read" is MiB read from disk during the cold run. "Rows read" includes
index entries on Postgres, and comes from the first run of each query. † marks a run that
failed, so its rows-read count is only what was read before it failed.

#### SF 1

| Query | Version | PG cold | PG warm median (p95) | CH cold | CH warm median (p95) | Speedup cold | Speedup warm | PG disk read cold (MB) | CH disk read cold (MB) | PG rows read | CH rows read |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| Q01 | original | 5.40 | 3.83 (3.94) | 0.41 | 0.31 (0.34) | 13.3x | 12.3x | 853 | 79.4 | 6,001,216 | 5,920,460 |
| Q02 | original | 3.65 | 0.23 (0.30) | 0.18 | 0.10 (0.12) | 20.0x | 2.2x | 186 | 9.30 | 381,516 | 1,820,060 |
| Q02 | rewritten | 4.69 | 0.69 (0.70) | 0.25 | 0.17 (0.18) | 18.8x | 4.0x | 178 | 9.20 | 278,409 | 1,820,060 |
| Q03 | original | 11.4 | 0.72 (0.73) | 0.53 | 0.41 (0.52) | 21.5x | 1.8x | 1,174 | 46.3 | 1,042,474 | 4,126,946 |
| Q03 | tuned | 11.4 | 0.72 (0.73) | 0.75 | 0.46 (0.48) | 15.2x | 1.6x | 1,174 | 47.1 | 1,042,474 | 4,126,946 |
| Q04 | original | 6.97 | 0.17 (0.18) | 0.31 | 0.16 (0.17) | 22.5x | 1.1x | 900 | 46.1 | 140,303 | 6,066,751 |
| Q04 | rewritten | 4.78 | 2.02 (2.03) | 0.45 | 0.25 (0.28) | 10.7x | 8.1x | 997 | 46.1 | 6,058,433 | 6,066,751 |
| Q05 | original | 7.99 | 0.33 (0.36) | 0.93 | 0.69 (0.70) | 8.6x | 0.5x | 778 | 93.7 | 665,930 | 6,388,842 |
| Q05 | tuned | 7.99 | 0.33 (0.36) | 1.57 | 1.26 (1.56) | 5.1x | 0.3x | 778 | 93.7 | 665,930 | 6,388,842 |
| Q06 | original | 12.1 | 0.39 (0.40) | 0.06 | 0.03 (0.03) | 208.2x | 12.9x | 781 | 9.30 | 909,455 | 909,455 |
| Q07 | original | 25.3 | 0.76 (0.81) | 0.59 | 0.45 (0.53) | 42.9x | 1.7x | 1,051 | 42.9 | 1,709,453 | 3,488,500 |
| Q08 | original | 6.46 | 0.18 (0.20) | 1.02 | 0.76 (1.05) | 6.4x | 0.2x | 600 | 120 | 380,032 | 6,818,533 |
| Q08 | tuned | 6.46 | 0.18 (0.20) | 1.43 | 0.92 (1.16) | 4.5x | 0.2x | 600 | 120 | 380,032 | 6,818,533 |
| Q09 | original | 12.0 | 1.47 (1.52) | 1.42 | 0.96 (1.00) | 8.4x | 1.5x | 1,257 | 152 | 1,200,973 | 8,511,240 |
| Q09 | tuned | 12.0 | 1.47 (1.52) | 1.81 | 1.68 (1.95) | 6.6x | 0.9x | 1,257 | 152 | 1,200,973 | 8,511,240 |
| Q10 | original | 8.25 | 0.44 (0.47) | 0.48 | 0.25 (0.33) | 17.3x | 1.8x | 953 | 50.6 | 435,926 | 6,214,045 |
| Q11 | original | 3.05 | 0.08 (0.09) | 0.16 | 0.05 (0.06) | 19.0x | 1.6x | 141 | 9.70 | 64,214 | 1,620,050 |
| Q12 | original | 4.13 | 0.75 (0.76) | 0.23 | 0.08 (0.10) | 17.9x | 8.9x | 1,022 | 23.4 | 6,032,207 | 4,074,528 |
| Q13 | original | 1.68 | 1.58 (1.62) | 0.74 | 0.28 (0.28) | 2.3x | 5.6x | 206 | 43.2 | 1,650,004 | 1,650,000 |
| Q14 | original | 4.71 | 0.17 (0.18) | 0.11 | 0.03 (0.04) | 44.1x | 5.1x | 348 | 2.80 | 275,987 | 282,073 |
| Q15 | original | 3.42 | 0.26 (0.29) | 0.13 | 0.03 (0.03) | 26.9x | 10.0x | 795 | 4.20 | 225,955 | 469,130 |
| Q16 | original | 0.81 | 0.62 (0.68) | 0.13 | 0.06 (0.06) | 6.1x | 10.7x | 48.3 | 5.90 | 1,030,004 | 1,010,000 |
| Q17 | original | 5.81 | 0.88 (0.89) | 0.49 | 0.32 (0.42) | 11.9x | 2.8x | 825 | 69.9 | 6,388,257 | 12,202,430 |
| Q17 | rewritten | 0.83 | 0.06 (0.06) | 0.30 | 0.12 (0.13) | 2.8x | 0.5x | 77.6 | 69.9 | 212,180 | 12,202,430 |
| Q18 | original | 9.88 | 7.50 (8.35) | 0.61 | 0.34 (0.36) | 16.3x | 21.7x | 1,026 | 62.4 | 19,654,052 | 13,652,430 |
| Q19 | original | 1.59 | 0.05 (0.05) | 0.42 | 0.16 (0.16) | 3.8x | 0.3x | 139 | 94.4 | 214,305 | 6,201,215 |
| Q20 | original | 15.8 | 0.10 (0.10) | 0.11 | 0.05 (0.05) | 140.1x | 2.2x | 538 | 19.3 | 272,787 | 1,919,480 |
| Q20 | rewritten | 5.83 | 0.12 (0.13) | 0.16 | 0.06 (0.08) | 36.0x | 1.9x | 522 | 19.5 | 278,475 | 1,919,480 |
| Q21 | original | 22.6 | 0.71 (0.74) | **OOM** | **OOM** | - | - | 1,178 | - | 2,388,736 | 29,412,930 † |
| Q21 | rewritten | 22.1 | 5.75 (6.23) | 1.13 | 0.75 (0.89) | 19.5x | 7.7x | 1,212 | 71.6 | 6,587,825 | 13,512,455 |
| Q22 | original | 0.30 | 0.10 (0.10) | 0.17 | 0.15 (0.16) | 1.8x | 0.7x | 40.5 | 17.9 | 312,620 | 1,800,000 |
| Q22 | rewritten | 0.37 | 0.21 (0.23) | 0.11 | 0.07 (0.09) | 3.3x | 3.0x | 40.4 | 10.6 | 1,650,000 | 1,800,000 |

#### SF 3

| Query | Version | PG cold | PG warm median (p95) | CH cold | CH warm median (p95) | Speedup cold | Speedup warm | PG disk read cold (MB) | CH disk read cold (MB) | PG rows read | CH rows read |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| Q01 | original | 12.8 | 10.5 (10.8) | 1.09 | 0.88 (1.06) | 11.7x | 11.8x | 2,446 | 246 | 17,996,610 | 17,756,038 |
| Q02 | original | 11.1 | 0.67 (0.67) | 0.47 | 0.28 (0.35) | 23.7x | 2.4x | 546 | 30.8 | 1,148,775 | 5,460,060 |
| Q02 | rewritten | 12.4 | 2.12 (2.12) | 0.72 | 0.50 (0.68) | 17.3x | 4.2x | 532 | 30.6 | 837,977 | 5,460,060 |
| Q03 | original | 15.1 | 6.05 (6.22) | 1.93 | 1.48 (1.55) | 7.9x | 4.1x | 3,108 | 152 | 19,352,978 | 12,370,393 |
| Q03 | tuned | 15.1 | 6.05 (6.22) | 1.89 | 1.63 (2.06) | 8.0x | 3.7x | 3,108 | 152 | 19,352,978 | 12,370,393 |
| Q04 | original | 22.6 | 17.9 (18.8) | 1.14 | 0.47 (0.49) | 19.8x | 38.2x | 2,689 | 158 | 423,588 | 18,185,025 |
| Q04 | rewritten | timeout | timeout | 1.44 | 0.85 (0.90) | - | - | - | 158 | 18,169,322 † | 18,185,025 |
| Q05 | original | 24.7 | 13.0 (13.1) | 3.35 | 3.16 (3.47) | 7.4x | 4.1x | 2,464 | 303 | 1,992,142 | 19,160,068 |
| Q05 | tuned | 24.7 | 13.0 (13.1) | 3.90 | 3.44 (3.47) | 6.3x | 3.8x | 2,464 | 303 | 1,992,142 | 19,160,068 |
| Q06 | original | 32.5 | 22.7 (22.9) | 0.16 | 0.11 (0.15) | 202.0x | 208.0x | 2,345 | 32.3 | 2,733,944 | 2,733,944 |
| Q07 | original | 162 | 119 (127) | 2.07 | 1.65 (1.95) | 78.1x | 72.1x | 6,833 | 137 | 5,160,620 | 10,444,237 |
| Q08 | original | 18.9 | 8.17 (12.6) | 3.31 | 2.86 (2.93) | 5.7x | 2.9x | 1,769 | 381 | 896,820 | 20,442,850 |
| Q08 | tuned | 18.9 | 8.17 (12.6) | 4.55 | 3.98 (4.25) | 4.2x | 2.1x | 1,769 | 381 | 896,820 | 20,442,850 |
| Q09 | original | 90.0 | 86.0 (88.3) | 7.12 | 4.27 (4.85) | 12.6x | 20.1x | 9,487 | 478 | 3,670,508 | 25,526,634 |
| Q09 | tuned | 90.0 | 86.0 (88.3) | 7.29 | 6.42 (6.55) | 12.3x | 13.4x | 9,487 | 464 | 3,670,508 | 25,526,634 |
| Q10 | original | 17.1 | 6.01 (6.12) | 1.08 | 0.69 (0.95) | 15.8x | 8.7x | 3,051 | 169 | 18,618,708 | 18,629,980 |
| Q11 | original | 9.69 | 0.26 (0.26) | 0.23 | 0.14 (0.15) | 41.8x | 1.8x | 426 | 29.6 | 193,814 | 4,860,050 |
| Q12 | original | 11.4 | 5.44 (5.97) | 0.74 | 0.19 (0.21) | 15.4x | 29.3x | 3,079 | 68.6 | 18,089,757 | 12,240,500 |
| Q13 | original | 4.98 | 4.52 (4.54) | 2.46 | 0.74 (0.76) | 2.0x | 6.1x | 618 | 127 | 4,950,004 | 4,950,000 |
| Q14 | original | 13.8 | 0.82 (0.83) | 0.10 | 0.06 (0.06) | 143.3x | 13.2x | 1,034 | 10.5 | 824,641 | 853,952 |
| Q15 | original | 43.1 | 45.0 (46.6) | 0.10 | 0.07 (0.07) | 439.6x | 681.2x | 1,882 | 12.4 | 678,083 | 1,422,640 |
| Q16 | original | 1.99 | 1.31 (1.33) | 0.22 | 0.15 (0.18) | 9.2x | 8.6x | 144 | 20.9 | 3,090,004 | 3,030,000 |
| Q17 | original | 17.5 | 5.66 (7.11) | 1.32 | 0.86 (0.87) | 13.3x | 6.6x | 2,489 | 227 | 19,158,796 | 36,593,218 |
| Q17 | rewritten | 2.55 | 0.11 (0.12) | 0.91 | 0.31 (0.31) | 2.8x | 0.4x | 230 | 235 | 635,978 | 36,593,218 |
| Q18 | original | 55.2 | 63.1 (67.2) | 1.42 | 1.01 (1.13) | 39.0x | 62.5x | 4,618 | 217 | 58,923,517 | 40,943,218 |
| Q19 | original | 5.33 | 0.15 (0.15) | 1.35 | 0.44 (0.62) | 3.9x | 0.3x | 428 | 305 | 644,270 | 18,596,609 |
| Q20 | original | 60.0 | 0.75 (0.77) | 0.38 | 0.13 (0.14) | 160.0x | 5.9x | 1,606 | 64.7 | 821,395 | 5,763,969 |
| Q20 | rewritten | 18.4 | 0.57 (0.58) | 0.32 | 0.12 (0.13) | 58.3x | 4.7x | 1,566 | 63.8 | 838,289 | 5,763,969 |
| Q21 | original | 37.4 | 25.3 (25.8) | **OOM** | **OOM** | - | - | 4,772 | - | 5,706,663 | 38,894,203 † |
| Q21 | rewritten | 174 | 197 (202) | 3.91 | 2.54 (3.15) | 44.6x | 77.9x | 7,077 | 241 | 19,759,210 | 40,523,243 |
| Q22 | original | 0.73 | 0.25 (0.27) | 0.41 | 0.21 (0.25) | 1.8x | 1.2x | 120 | 48.0 | 937,841 | 5,400,000 |
| Q22 | rewritten | 1.11 | 0.72 (0.77) | 0.32 | 0.15 (0.17) | 3.5x | 4.7x | 120 | 26.5 | 4,950,000 | 5,400,000 |

#### SF 10

| Query | Version | PG cold | PG warm median (p95) | CH cold | CH warm median (p95) | Speedup cold | Speedup warm | PG disk read cold (MB) | CH disk read cold (MB) | PG rows read | CH rows read |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| Q01 | original | 36.9 | 36.6 (37.2) | 2.99 | 2.58 (2.86) | 12.3x | 14.2x | 7,997 | 771 | 59,986,053 | 59,147,358 |
| Q02 | original | 38.7 | 10.4 (13.4) | 1.27 | 0.94 (1.07) | 30.4x | 11.1x | 1,853 | 93.7 | 3,705,311 | 18,200,060 |
| Q02 | rewritten | 44.5 | 13.6 (15.1) | 2.17 | 2.00 (2.20) | 20.5x | 6.8x | 1,771 | 93.7 | 2,805,821 | 18,200,060 |
| Q03 | original | 31.3 | 25.8 (26.0) | **OOM** | **OOM** | - | - | 10,252 | - | 76,486,060 | 37,548,648 † |
| Q03 | tuned | 31.3 | 25.8 (26.0) | 9.40 | 9.43 (9.81) | 3.3x | 2.7x | 10,252 | 463 | 76,486,060 | 41,147,434 |
| Q04 | original | 91.4 | 72.1 (76.0) | 2.84 | 1.60 (1.75) | 32.2x | 45.2x | 8,995 | 468 | 1,406,498 | 60,575,876 |
| Q04 | rewritten | 108 | timeout | 4.00 | 2.75 (2.88) | 27.0x | - | 10,760 | 468 | 60,559,723 † | 60,575,876 |
| Q05 | original | 40.7 | 33.2 (34.7) | **OOM** | **OOM** | - | - | 10,271 | - | 63,862,079 | 27,420,762 † |
| Q05 | tuned | 40.7 | 33.2 (34.7) | 22.5 | 25.0 (29.9) | 1.8x | 1.3x | 10,271 | 1,449 | 63,862,079 | 123,848,053 |
| Q06 | original | 89.9 | 78.5 (81.1) | 0.37 | 0.18 (0.18) | 245.6x | 429.1x | 7,824 | 92.9 | 9,099,165 | 9,099,165 |
| Q07 | original | 73.2 | 72.7 (81.3) | 6.74 | 7.33 (8.82) | 10.9x | 9.9x | 10,369 | 432 | 34,830,491 | 34,830,375 |
| Q08 | original | 70.0 | 55.5 (56.0) | **OOM** | **OOM** | - | - | 6,857 | - | 4,407,161 | 31,790,748 † |
| Q08 | tuned | 70.0 | 55.5 (56.0) | 35.0 | 28.1 (28.9) | 2.0x | 2.0x | 6,857 | 1,578 | 4,407,161 | 128,129,672 |
| Q09 | original | 507 | 497 (502) | **OOM** | **OOM** | - | - | 49,465 | - | 19,784,944 | 31,382,137 † |
| Q09 | tuned | 507 | 497 (502) | 32.0 | 38.1 (40.8) | 15.8x | 13.0x | 49,465 | 2,080 | 19,784,944 | 145,072,129 |
| Q10 | original | 50.0 | 39.3 (39.7) | 6.49 | 5.54 (5.87) | 7.7x | 7.1x | 10,194 | 507 | 62,059,294 | 62,066,971 |
| Q11 | original | 30.0 | 1.31 (1.33) | 0.97 | 0.77 (0.81) | 31.0x | 1.7x | 1,420 | 95.7 | 751,976 | 16,200,050 |
| Q12 | original | 35.3 | 31.5 (31.7) | 1.98 | 0.60 (0.78) | 17.9x | 52.3x | 10,274 | 200 | 60,296,859 | 40,804,316 |
| Q13 | original | 115 | 42.2 (42.3) | 12.4 | 2.62 (2.76) | 9.3x | 16.1x | 2,182 | 427 | 16,500,004 | 16,500,000 |
| Q14 | original | 44.2 | 32.0 (32.4) | 0.26 | 0.19 (0.21) | 168.0x | 166.5x | 3,443 | 18.7 | 2,749,227 | 2,761,856 |
| Q15 | original | 205 | 191 (194) | 0.25 | 0.24 (0.34) | 831.0x | 797.5x | 5,399 | 31.0 | 2,265,715 | 4,638,368 |
| Q16 | original | 5.96 | 4.45 (4.58) | 0.64 | 0.41 (0.42) | 9.3x | 10.8x | 485 | 58.4 | 10,300,004 | 10,100,000 |
| Q17 | original | 48.3 | 19.6 (19.9) | 4.02 | 3.58 (3.88) | 12.0x | 5.5x | 8,539 | 696 | 63,890,269 | 121,972,104 |
| Q17 | rewritten | 8.05 | 0.48 (0.49) | 2.51 | 1.54 (1.82) | 3.2x | 0.3x | 776 | 696 | 2,122,774 | 121,972,104 |
| Q18 | original | 140 | 128 (130) | 4.90 | 4.69 (6.45) | 28.7x | 27.3x | 18,058 | 642 | 121,472,736 | 136,472,104 |
| Q19 | original | 15.4 | 0.69 (0.70) | 3.41 | 1.41 (1.43) | 4.5x | 0.5x | 1,385 | 935 | 2,142,588 | 61,986,052 |
| Q20 | original | 720 | 683 (688) | 0.82 | 0.28 (0.29) | 876.1x | 2465.9x | 17,622 | 199 | 2,369,181 | 19,199,190 |
| Q20 | rewritten | 63.6 | 51.3 (52.6) | 0.87 | 0.43 (0.43) | 73.1x | 119.0x | 5,388 | 199 | 2,793,235 | 19,199,190 |
| Q21 | original | 326 | 317 (319) | **OOM** | **OOM** | - | - | 35,810 | - | 20,655,010 | 49,563,630 † |
| Q21 | rewritten | 1,317 | 1,358 (1,368) | 19.5 | 19.9 (20.1) | 67.6x | 68.3x | 39,232 | 734 | 65,855,712 | 135,072,129 |
| Q22 | original | 2.23 | 0.81 (0.82) | 0.80 | 0.54 (0.55) | 2.8x | 1.5x | 400 | 162 | 3,126,781 | 18,000,000 |
| Q22 | rewritten | 3.78 | 2.87 (2.91) | 0.66 | 0.49 (0.51) | 5.7x | 5.8x | 400 | 88.5 | 16,500,000 | 18,000,000 |

## 5. ClickHouse join tuning

### The problem

With the default settings, five ClickHouse queries fail at SF 10 on the memory limit: Q3, Q5,
Q8, Q9 and the original Q21. All are large joins. ClickHouse's default join (`hash` /
`parallel_hash`) builds the whole right-hand side of each join as an in-memory hash table.
At SF 10 the tables on `orders` or `lineitem` don't fit in the 3 GB per-query cap. Unlike
GROUP BY and ORDER BY, which spill to disk above 1.5 GB in this profile, the default join
never spills.

ClickHouse has join algorithms that can work within a memory budget:

| Setting | How it works |
|---|---|
| `auto` + `max_bytes_in_join` | Starts as a hash join; switches to a merge join if the hash table passes the threshold |
| `grace_hash` + `max_bytes_in_join` | Splits both sides into buckets by key; buckets that don't fit are written to disk and joined one at a time |
| `partial_merge` | Sorts the right side in blocks (spillable) and merges the left side against them |
| `full_sorting_merge` | Sorts both sides (external sort, spillable) and merge-joins them |

The tuning had two questions to answer:

1. Which settings make the failing queries complete?
2. Can that setting go in the server profile, or does it hurt the queries that already work?

Both steps are reproducible with [scripts/join_tuning.py](scripts/join_tuning.py). The raw
output is in [results/join_tuning/](results/join_tuning/). Those logs came from earlier,
exploratory versions of the same script, using the same procedure. All runs are at SF 10 with a
warm cache, with the setting passed per query, so the server profile was never changed.

### Step 1: the failing queries

Each setting ran twice on the five failing queries, and the faster run is reported. "Spilled"
is MB written to disk.

| Query | Default | `auto` 1 GB | `grace_hash` 1 GB | `partial_merge` | `full_sorting_merge` |
|---|--:|--:|--:|--:|--:|
| Q3 | OOM after 17 s | 27.6 s | 22.7 s | 25.9 s | **8.5 s** |
| Q5 | OOM after 7 s | 83.0 s | 46.3 s | 82.6 s | **23.4 s** |
| Q8 | OOM after 5 s | 72.6 s | 47.5 s | 62.5 s | **24.8 s** |
| Q9 | OOM after 4 s | 109.6 s ¹ | 71.6 s ¹ | 86.3 s | **32.6 s** |
| Q21 original | OOM | OOM | OOM | OOM | OOM |
| Spilled (Q5 / Q8 / Q9) | 0 | 1,442 / 1,467 / 3,037 MB | 1,017 / 951 / 1,718 MB | 1,586 / 1,539 / 2,098 MB | 676 / 629 / 750 MB |

¹ In the first pass, Q9 failed after 0.2 s under `auto` and `grace_hash`, because it ran
right after a query that had just hit the memory limit. Rerun with a freshly restarted server,
both complete; the table shows the rerun.

- **`full_sorting_merge` is the clear winner on the failing queries**: 3–4x faster than the
  other options, the least spilled to disk, and all 3 cores busy (about 2.8 cores against
  1.1–1.4 for the others).
- **The original Q21 fails under every setting.** The join setting doesn't help its
  correlated `EXISTS`/`NOT EXISTS`, so the CTE rewrite is the only way to run it on ClickHouse.
- Every completed result matches Postgres, except Q8's known decimal truncation.

### Step 2: the cost as a server-wide setting

A profile setting applies to every query, so each candidate ran on all 27 query versions
(the original Q21 excluded). Each version ran once as a warm-up, checked against Postgres's
result, then 3 more times, and the median is reported. The baseline is the default's warm
median from the main SF 10 run made before the tuned versions existed
(`results/queries_clickhouse_sf10_20261028T030928Z.json`, kept for that reason).

| Query | Default | `full_sorting_merge` | `auto` 1 GB | `auto` 1.5 GB |
|---|--:|--:|--:|--:|
| Q1 | 2.57 s | 2.82 s (1.10x) | 2.58 s (1.00x) | 2.63 s (1.03x) |
| Q2 | 0.88 s | 1.60 s (1.82x) | 1.37 s (1.55x) | 1.74 s (1.97x) |
| Q2 rewritten | 2.01 s | 8.64 s (**4.29x**) | 2.06 s (1.02x) | 2.00 s (0.99x) |
| Q3 | OOM | **8.15 s** | 30.50 s | 29.83 s |
| Q4 | 1.57 s | 6.71 s (**4.26x**) | 20.58 s (**13.08x**) | 4.43 s (2.82x) |
| Q4 rewritten | 2.73 s | 7.39 s (2.71x) | 10.95 s (**4.02x**) | 5.00 s (1.83x) |
| Q5 | OOM | **27.55 s** | 79.63 s | 92.76 s |
| Q6 | 0.24 s | 0.19 s (0.81x) | 0.19 s (0.80x) | 0.19 s (0.79x) |
| Q7 | 5.95 s | 13.30 s (2.23x) | 43.68 s (**7.34x**) | 7.84 s (1.32x) |
| Q8 | OOM | **26.07 s** | 71.76 s | 83.87 s |
| Q9 | OOM | **34.18 s** | 88.62 s | **OOM** |
| Q10 | 5.51 s | 7.50 s (1.36x) | 25.20 s (**4.57x**) | 26.68 s (**4.84x**) |
| Q11 | 0.76 s | 2.01 s (2.65x) | 0.73 s (0.96x) | 1.07 s (1.41x) |
| Q12 | 0.59 s | 5.68 s (**9.67x**) | 0.73 s (1.24x) | 0.73 s (1.25x) |
| Q13 | 2.59 s | 13.98 s (**5.40x**) | 42.00 s (**16.22x**) | **OOM** |
| Q14 | 0.16 s | 0.20 s (1.28x) | 0.31 s (2.00x) | 0.32 s (2.07x) |
| Q15 | 0.20 s | 0.19 s (0.99x) | 0.20 s (1.03x) | 0.24 s (1.24x) |
| Q16 | 0.41 s | 0.64 s (1.55x) | 0.45 s (1.10x) | 0.42 s (1.03x) |
| Q17 | 3.31 s | 19.85 s (**5.99x**) | 3.97 s (1.20x) | 3.85 s (1.16x) |
| Q17 rewritten | 1.55 s | 28.57 s (**18.41x**) | 1.60 s (1.03x) | 1.58 s (1.02x) |
| Q18 | 4.16 s | 4.45 s (1.07x) | **OOM** | 4.54 s (1.09x) |
| Q19 | 1.40 s | 3.07 s (2.20x) | 1.69 s (1.21x) | 1.57 s (1.13x) |
| Q20 | 0.29 s | 0.29 s (1.00x) | 0.28 s (1.00x) | 0.29 s (1.02x) |
| Q20 rewritten | 0.42 s | 1.68 s (**4.00x**) | 0.47 s (1.13x) | 0.43 s (1.02x) |
| Q21 rewritten | 19.03 s | 33.88 s (1.78x) | 23.92 s (1.26x) | 21.74 s (1.14x) |
| Q22 | 0.54 s | 1.76 s (3.25x) | 0.82 s (1.51x) | 0.62 s (1.15x) |
| Q22 rewritten | 0.51 s | 0.71 s (1.40x) | 0.87 s (1.72x) | 0.65 s (1.29x) |
| **Failures** | **4** | **0** | **1** (Q18) | **2** (Q9, Q13) |
| **Slowed by more than 2x** | – | **12** of 23 | **5** of 22 | **3** of 22 |

(x) is the time relative to the default; bold marks failures and slowdowns of 4x or more.

None of the three is acceptable as a server-wide setting:

- **`full_sorting_merge`** completes everything, but sorting both sides of every join is
  wasted work when the hash table would have fit. 12 of the 23 versions that already worked
  get more than 2x slower, up to 18x (the Q17 rewrite, whose small join turns into a sort of
  60M `lineitem` rows).
- **`auto` with a 1 GB threshold** switches joins that would have fit in memory to the slower
  merge join (Q13 16x, Q4 13x, Q7 7x), and Q18 now fails.
- **`auto` with a 1.5 GB threshold** reduces the slowdowns, but lets the hash table grow so
  close to the limit that the switch itself doesn't fit: Q9 and Q13 fail (Q9 would have
  needed 3.34 GiB).

All four OOM failures under `auto` hit the **server-wide** limit
(`max_server_memory_usage`, 3.5 GB), not the per-query one. ClickHouse also doesn't release
memory immediately after a heavy query; small queries showed 2–3 GB of container working memory
after one. So whether a query fails depends partly on what ran before it, which makes any
global threshold fragile on 4 GB.

### Decision: a per-query setting

The server profile keeps the default join. Only Q3, Q5, Q8 and Q9 get
`SETTINGS join_algorithm = 'full_sorting_merge'`, as the tuned versions in
[queries/clickhouse/tuned/](queries/clickhouse/tuned/). This:

- uses the fastest algorithm for the four failing queries
- leaves every other query exactly as it was
- matches what an operations team would do after a migration: find the reports that fail and
  tune them one at a time. That tuning work is a real migration cost, and the benchmark now
  measures it.

The original Q21 can't be fixed by a setting, so it stays rewrite-only.

### Tuned results

The tuned versions ran in the full benchmark at every scale (cold run plus 5 warm runs); the
complete numbers are in the per-scale tables above. Times are warm medians.

| Query | SF 1: CH original → tuned | SF 3: CH original → tuned | SF 10: CH tuned | SF 10: PG original | SF 10 speedup (warm) | SF 10 spilled (cold) |
|---|--:|--:|--:|--:|--:|--:|
| Q3 | 0.41 → 0.46 s | 1.48 → 1.63 s | 9.43 s | 25.8 s | 2.7x | 0 MB |
| Q5 | 0.69 → 1.26 s | 3.16 → 3.44 s | 25.0 s | 33.2 s | **1.3x** | 706 MB |
| Q8 | 0.76 → 0.92 s | 2.86 → 3.98 s | 28.1 s | 55.5 s | 2.0x | 590 MB |
| Q9 | 0.96 → 1.68 s | 4.27 → 6.42 s | 38.1 s | 497 s | 13.0x | 711 MB |

- **All four complete at SF 10 and match Postgres** (Q8 apart from the decimal truncation).
- **They're among ClickHouse's weakest queries.** Across the 21 queries that complete at
  SF 10, ClickHouse's median warm speedup is 11x, but the tuned Q5, Q8 and Q3 are only 1.3x,
  2.0x and 2.7x faster. Only Q19 (where Postgres is faster), Q22 and Q11 come out lower. Q9
  stays well ahead because Postgres itself struggles with it (8 minutes).
- **The setting costs time when it isn't needed.** At SF 1 and SF 3, where the default join
  fits in memory, the tuned versions are 1.1–1.8x slower than the originals. In production the
  setting should only go on reports that need it, and be reviewed as data grows.
- **Spilling shows up as extra I/O.** At SF 10, the tuned Q5, Q8 and Q9 write 590–711 MB of
  sorted runs to disk and read them back. ClickHouse reports 4x more rows read for them than
  their failed originals got through before failing (124–145 M against 27–32 M), consistent
  with re-reading the spilled runs. The cold disk read also roughly triples (1.4–2.1 GB, against
  0.5–0.7 GB for the other large queries).

**For the risk assessment:** on this memory-to-data ratio, no single ClickHouse configuration
runs all 22 reports both correctly and fast. Large joins need per-query tuning (and some need
rewriting), or each server needs more memory per query. In production that is either an
operations cost or a hardware cost, and it should go into the TCO comparison.

## 6. Write workload: late corrections, inserts and upserts

The brief calls out late-arriving financial corrections as a known weakness of columnar
engines, so the benchmark also measures writes. The 22 TPC-H queries are read-only. TPC-H's own
refresh functions (RF1 inserts new orders, RF2 deletes old ones) have no UPDATE and no upsert.
They also aren't in the DuckDB `tpch` extension. So the write workload is generated here.

### Generated once, committed to the repository

[scripts/gen_workload.py](scripts/gen_workload.py) reads `data/sf<N>/` and writes the changes
to `workloads/sf<N>/` as zstd Parquet, together with a `manifest.json`. Unlike `data/`, this
folder is committed, so:

- **both engines apply exactly the same changes**, row for row
- **the workload is fixed and reviewable**: the files say exactly what each run changes, and
  the manifest records the seed, the number of rows per scenario, and a SHA-256 for each file
- **nothing is generated at benchmark time**

```bash
python scripts/gen_workload.py --sf 1   # also --sf 3, --sf 10 (needs data/sf<N>/ first)
```

Every random choice is a hash of the row key, a purpose label and a fixed seed
(`hash(key, 'discount', SEED)`), not a stateful random generator. The output doesn't depend on
thread count or row order, and rerunning the script produces byte-identical files.

### Scenarios

Each scenario has **10 runs**. A run is one unit of work, applied and timed as one
measurement. Runs never overlap: each row is updated, deleted or upserted in at most one run of
one scenario, and corrections never touch an order that a delete removes. So the runs can be
applied one after the other without affecting each other.

| Scenario | Unit per run | Models |
|---|---|---|
| `insert_batch` | SF × 1,500 orders, with their lines (0.1% of `orders`, the TPC-H RF1 size) | The daily load, as one statement |
| `insert_small` | 100 orders, to be sent one order per statement | A trickle of transactions; ClickHouse creates a part per insert |
| `update_point` | 1 lineitem | A single late correction |
| `update_100` / `update_1k` / `update_10k` | 100 / 1,000 / 10,000 lineitems, scattered across all 7 years | A correction batch |
| `delete_point` / `delete_100` / `delete_1k` | 1 / 100 / 1,000 orders and all their lines | Reversals and cancellations |
| `upsert_1k` | 1,000 lineitems: 500 corrections of existing lines, 500 new lines | A late feed that mixes corrections and new data (MERGE) |

### Files

| File | Contents | SF 1 rows | SF 3 rows | SF 10 rows |
|---|---|--:|--:|--:|
| `insert_orders.parquet` | Full `orders` rows | 16,000 | 46,000 | 151,000 |
| `insert_lineitem.parquet` | Full `lineitem` rows of the new orders | 64,127 | 184,206 | 603,600 |
| `update_lineitem.parquet` | Full `lineitem` rows **after** the correction, plus `correction_type` | 111,010 | 111,010 | 111,010 |
| `delete_orders.parquet` | `o_orderkey`, `o_orderdate` | 11,010 | 11,010 | 11,010 |
| `delete_lineitem.parquet` | `l_orderkey`, `l_linenumber`, `l_shipdate` of the deleted orders' lines | 43,842 | 43,891 | 44,066 |
| `upsert_lineitem.parquet` | Full `lineitem` rows, plus `is_new` and `correction_type` | 10,000 | 10,000 | 10,000 |
| **Folder size** | | 4.8 MB | 7.5 MB | 17 MB |

Every file starts with `scenario` and `run_id`, and the other columns have the same names and
types as the TPC-H tables. Updates and upserts store the **full row**, not only the changed
columns. Postgres and a ClickHouse `UPDATE` use only the changed columns. A ReplacingMergeTree
table needs the whole new row version. Deletes store the date column of the sort key, because
a ReplacingMergeTree "delete" is an insert of a tombstone row with the same key.

### How the rows are built

- **Corrections** change exactly one money column per line: the discount (0.00–0.10), the tax
  (0.00–0.08) or the quantity (1–50). A quantity change also rescales `l_extendedprice`, which
  TPC-H defines as quantity × the part's retail price. The new value always differs from the old
  one. The sort key columns (`l_shipdate`, `l_orderkey`, `l_linenumber`) never change, so a
  corrected row keeps its key and partition on ClickHouse.
- **New orders** use the order keys that `dbgen` leaves unused. It only uses keys with
  `key % 32` in 0–7, and TPC-H RF1 fills the same gaps. Customer, clerk, priority, line count,
  part, supplier, price, discount, tax, dates, flags, ship mode and instructions follow the
  formulas in TPC-H spec section 4.2.3. The supplier and retail price formulas were checked
  against the generated data. `o_totalprice` and `o_orderstatus` are derived from the new lines.
  Comments are sampled from a pool of 10,000 real comments.
- **New order dates fall in the last 30 days of the TPC-H range** (1998-07-04 to 1998-08-02),
  which models a daily load. This differs from RF1, which spreads new orders over all 7 years.
  As a result, every new line lands in the 1998 partition on ClickHouse. A single INSERT into a
  single partition is atomic there.
- **New lines in upserts** are added to existing orders that have fewer than 7 lines, with the
  next line number. They model a line that arrives after its order was loaded.

### Simplifications

- **`o_totalprice` isn't recomputed** when a correction or a new upsert line changes an order's
  lines. The workload only changes `lineitem`. Keeping the order total in step would be a
  two-table change, which ClickHouse can't apply atomically. That is part of the atomicity risk
  covered in the write benchmark.
- **Bulk restatements** (for example, re-pricing every line of one year) are defined as a SQL
  rule in the write benchmark, not stored as rows. At SF 10 one year is about 9 million lines.
- **ClickHouse mutations (`ALTER TABLE … UPDATE/DELETE`) aren't tested.** They rewrite whole
  parts in the background and are meant for occasional large-scale changes, not for routine
  corrections. ClickHouse is tested with lightweight `UPDATE` and `DELETE`, with
  ReplacingMergeTree (insert a new version or a tombstone), and with `REPLACE PARTITION` for
  bulk restatements.

## 7. Write benchmark

[scripts/run_writes.py](scripts/run_writes.py) applies the workload from section 6 to one
engine and measures every run.

```bash
python scripts/run_writes.py --engine postgres --sf 1
python scripts/run_writes.py --engine clickhouse --sf 1          # both ClickHouse methods
python scripts/run_writes.py --engine clickhouse --sf 1 --method replacing --runs 2  # smoke test
python scripts/run_writes.py --compare --sf 1                    # final table checksums
```

### Methods

Each method applies the same changes in the way that engine is normally used.

| Scenario | Postgres | ClickHouse `lightweight` (MergeTree) | ClickHouse `replacing` (ReplacingMergeTree) |
|---|---|---|---|
| Inserts | `COPY` (batch) or `INSERT` (one order per transaction) | `INSERT` | `INSERT` with a new `_version` |
| Updates | `UPDATE … FROM (VALUES …)` | Lightweight `UPDATE`, one statement per batch | `INSERT` of the new row version |
| Deletes | `DELETE` lines and orders in one transaction | Lightweight `DELETE`, one statement per table | `INSERT` of tombstones (`_is_deleted = 1`) |
| Upsert | `MERGE` | `DELETE` the keys, then `INSERT` all rows (no MERGE in ClickHouse) | `INSERT` |
| Restatement | `UPDATE … WHERE` the year | Rebuild the year in a staging table, then `REPLACE PARTITION` | Same, reading with `FINAL` |
| Reads (Q1, Q6) | as is | as is | `final = 1` |

**Restatement** is the bulk case: every line shipped in 1993, 1994 or 1995 (one year per run)
gets a tax rate one point higher, about 9 million lines per year at SF 10.

Each Postgres run is one transaction, so it is atomic. On ClickHouse, a run that touches two
tables, or deletes and then inserts, is several separate statements. A reader can see it half
applied, which is the atomicity gap discussed for the risk assessment.

### Isolation from the read benchmark

The writes go to copies of `orders` and `lineitem` that are rebuilt at the start of every
invocation: Postgres schema `w_sf<N>`, ClickHouse databases `w_sf<N>_lw` and `w_sf<N>_rmt`. The
`sf<N>` data used by the read benchmark is never changed.

- The Postgres copy gets the same primary keys and indexes as the original, a
  `VACUUM (ANALYZE)` and a checkpoint. **Autovacuum is off on the copies**, so maintenance runs
  only when the benchmark asks for it and its cost is measured, not mixed into random runs.
- The ClickHouse `lightweight` copy is created with `enable_block_number_column` and
  `enable_block_offset_column`. Lightweight UPDATE needs them, and they can only be set when a
  table is created. **Adopting lightweight updates means rebuilding the table**, as does
  switching to ReplacingMergeTree. The `replacing` copy adds `_version UInt64` and
  `_is_deleted UInt8` columns. The sort key and partitioning are unchanged.

### Phases and metrics

1. **Setup**: build the copies.
2. **Baseline reads**: Q1 and Q6, one warm-up run and then 5 timed runs.
3. **Scenarios**: the 10 runs of each scenario (3 for the restatement), in the order of
   section 6. After each scenario the engine **settles**:
   - Postgres runs `CHECKPOINT`, which writes the dirty data pages to disk.
   - ClickHouse waits until no merges or mutations have run for 3 seconds.

   The settle step's time, CPU and bytes written are the background cost the scenario left
   behind, reported apart from the runs themselves.
4. **Reads after writes**: Q1 and Q6 again, before any cleanup. This shows the read penalty of
   dead rows, delete masks, patch parts and `FINAL`.
5. **Cleanup**:
   - Postgres: `VACUUM (ANALYZE)` and `CHECKPOINT`.
   - ClickHouse: `OPTIMIZE TABLE … FINAL`, which applies masks, patches and tombstones.
6. **Reads after cleanup**.

Per run:
- **Time**: median, p95 and minimum of the client-side wall time of all statements in the run.
- **Resources**: CPU time, average busy cores, peak memory (total and working) and disk bytes
  read and written. These come from the container's cgroup, as in the read benchmark.
- **Engine counters**:
  - Postgres: rows read, inserted, updated, HOT-updated and deleted, and buffer bytes accessed
    (`pg_stat_*` deltas).
  - ClickHouse: rows and bytes read and written (`system.query_log`).
- **Storage** after each scenario and after cleanup:
  - Postgres: table and index size, live and dead rows.
  - ClickHouse: bytes on disk, active parts, patch parts.

Errors and timeouts (30 min) are recorded as results, as in the read benchmark.

**Correctness check:** the script takes a checksum of both tables after the writes and after
the cleanup: row counts, plus the sums of quantity, price, discount, tax and order total.
`--compare` checks that every method ended with the same data. At SF 1, all three methods
match.

Results go to `results/writes_<engine>_<method>_sf<N>_<ts>.json` (every run) and
`results/writes.csv` (one line per scenario, cleanup and read phase).

### Pitfalls found while building it

These are worth knowing for any migration:

- **Decimal arithmetic with a bare literal silently loses money on ClickHouse.** `0.01` is a
  Float64 there, so `l_tax + 0.01` is computed in floating point. When the result is stored back
  into a `Decimal(15,2)` column it is **truncated**: 0.06 + 0.01 = 0.0699… becomes 0.06. The
  first version of the restatement left about 200,000 of the 1.8 million restated lines (SF 1,
  2 runs) at their old tax rate, with no error. Postgres
  treats `0.01` as an exact numeric. The fix is an explicit decimal, `toDecimal32('0.01', 2)`.
  **Every migrated report and correction script that does decimal arithmetic needs this
  review.**
- **ReplacingMergeTree tombstones survive `OPTIMIZE … FINAL`.** A deleted row's tombstone stays
  in the table after the merge, and a read without `FINAL` counts it. So `final = 1` is needed
  for good, not just until merges catch up. Removing tombstones needs `OPTIMIZE … FINAL CLEANUP`,
  which is behind an experimental setting.
- **ClickHouse UPDATE has no `FROM` or join.** A batch of row-specific corrections is written as
  one statement that maps each row key to its new value with `transform()`. A 10,000-row batch
  needed higher parser limits (`max_query_size`, `max_ast_elements`).
- **ClickHouse's memory accounting drifted until ordinary statements failed.** The first full
  run failed at every scale, and every error was the 3.5 GB server-wide memory limit:
  - Even a one-order insert, a one-row delete and copying the tables at setup failed.
  - At the time of each error, the server's real memory use (RSS) was only 1.0–1.7 GB.
  - The cause: ClickHouse kept counting memory that background merges and mutations had
    already freed. On an idle server with nothing merging, it still charged 2.07 GiB to merges,
    and lightweight DELETE starts a mutation on every part, so the drift grew quickly.

  The fix is the server setting `memory_worker_correct_memory_tracker = 1`, now in
  [clickhouse/config.d/memory.xml](clickhouse/config.d/memory.xml). It periodically resets the
  count to the real RSS. All ClickHouse results below were run with it, with no failures.
  - **Effect on the read benchmark:** none of its results depend on the drift. Its SF 10 memory
    failures (Q3, Q5, Q8, Q9, Q21) all hit the per-query 3 GB limit in the first SF 10 run.
  - **For the risk assessment:** with the default configuration, a memory-capped ClickHouse
    stopped accepting writes under a correction workload. That is the kind of setting an
    operations team only finds in production.

### Results

All three methods ended every scale with identical data (`--compare`):
- **SF 1 and SF 3:** the checksums match exactly.
- **SF 10:** the two ClickHouse methods match each other. Postgres differs only in the tax
  total, by exactly the restatement that timed out and was rolled back.

Times are client-side medians, with the p95 in parentheses, over 10 runs (3 for the
restatement). Raw data: `results/writes_*.json` and `results/writes.csv`.

#### SF 10: time per run

| Scenario | Postgres | ClickHouse lightweight | ClickHouse replacing |
|---|--:|--:|--:|
| `insert_batch` (15,000 orders, ~60,000 lines) | 36.0 s (79.0) | **0.20 s** (0.52) | 0.30 s (0.63) |
| `insert_small` (100 orders, one per statement) | **0.71 s** (0.80) | 1.29 s (3.33) | 1.39 s (1.89) |
| `update_point` (1 line) | **0.007 s** (0.012) | 0.045 s (0.149) | 0.032 s (0.098) |
| `update_100` | 0.21 s (0.25) | 0.21 s (0.33) | **0.10 s** (0.29) |
| `update_1k` | 1.55 s (1.70) | 1.15 s (1.38) | **0.055 s** (0.12) |
| `update_10k` | 17.0 s (23.5) | 23.1 s (23.5) | **0.11 s** (0.20) |
| `delete_point` (1 order) | **0.006 s** (0.008) | 0.92 s (6.09) | 0.055 s (0.11) |
| `delete_100` | **0.21 s** (0.28) | 4.43 s (5.29) | 0.24 s (0.37) |
| `delete_1k` | 1.67 s (1.90) | 19.3 s (20.0) | **0.21 s** (0.39) |
| `upsert_1k` | 1.37 s (1.56) | 6.03 s (6.71) | **0.10 s** (0.23) |
| `restatement` (9.1 million lines) | **timeout** ×3 (> 30 min) | **13.9 s** (20.6) | 21.4 s (24.1) |

#### SF 10: CPU per scenario

Total container CPU time for the 10 runs (3 for the restatement), plus the background work left
behind (checkpoint or merges):

| Scenario | Postgres | ClickHouse lightweight | ClickHouse replacing |
|---|--:|--:|--:|
| `insert_batch` | 134 s | 11 s | 11 s |
| `insert_small` | 2 s | 22 s | 24 s |
| `update_point` / `update_100` / `update_1k` / `update_10k` | 0 / 1 / 7 / 66 s | 3 / 7 / 33 / **655 s** | 3 / 5 / 4 / 6 s |
| `delete_point` / `delete_100` / `delete_1k` | 0 / 1 / 7 s | 35 / 115 / **535 s** | 4 / 9 / 9 s |
| `upsert_1k` | 6 s | 173 s | 5 s |
| `restatement` | 2,160 s (3 timeouts) | 75 s | 94 s |

Disk writes are only comparable on Postgres. Its settle step is a `CHECKPOINT`, so everything is
on disk when it is measured. ClickHouse doesn't fsync new parts by default, so the kernel writes
them back later, often outside the measured window. On Postgres at SF 10:
- `update_10k` wrote 4.7 GB: WAL, heap and index pages, about 47 KB per updated line.
- The three restatement attempts wrote **200 GB**, and all of it was rolled back.

#### How the time grows with scale (medians)

| Scenario | Postgres SF 1 → 3 → 10 | ClickHouse lightweight | ClickHouse replacing |
|---|---|---|---|
| `insert_batch` | 0.61 → 2.63 → 36.0 s | 0.07 → 0.07 → 0.20 s | 0.05 → 0.13 → 0.30 s |
| `update_10k` | 2.36 → 7.12 → 17.0 s | 4.52 → 9.95 → 23.1 s | 0.12 → 0.07 → 0.11 s |
| `delete_point` | 0.005 → 0.006 → 0.006 s | 0.27 → 0.27 → 0.92 s | 0.04 → 0.04 → 0.06 s |
| `delete_1k` | 0.49 → 1.31 → 1.67 s | 9.53 → 11.9 → 19.3 s | 0.15 → 0.16 → 0.21 s |
| `upsert_1k` | 0.32 → 0.66 → 1.37 s | 3.14 → 3.70 → 6.03 s | 0.09 → 0.09 → 0.10 s |
| `restatement` (one year) | 28.8 → 236 → timeout | 0.82 → 3.04 → 13.9 s | 1.22 → 4.10 → 21.4 s |

The `insert_batch` and `restatement` units grow with the scale factor. The other scenarios
change the same number of rows at every scale, so their growth is the cost of the table size
alone.

#### SF 10: reads and maintenance

| | Postgres | ClickHouse lightweight | ClickHouse replacing |
|---|--:|--:|--:|
| Q1: baseline → after writes → after cleanup | 36.4 → 49.5 → 40.1 s | 2.77 → 3.19 → 2.99 s | 5.56 → 5.99 → 6.53 s |
| Q6: baseline → after writes → after cleanup | 88.4 → 126 → 106 s | 0.36 → 0.26 → 0.37 s | 0.80 → 0.72 → 0.81 s |
| Size: copy → after writes → after cleanup | 14.3 → 19.6 → 17.0 GB | 3.34 → 3.38 → 3.36 GB | 3.34 → 3.37 → 3.35 GB |
| Cleanup | VACUUM + CHECKPOINT: 466 s, 21.8 GB written | OPTIMIZE FINAL: 87 s | OPTIMIZE FINAL: 125 s |

Notes:
- On Postgres, the dead rows (19.6 million) come mostly from the restatement attempts that
  timed out: a rolled-back UPDATE still leaves its new row versions behind as dead rows.
- VACUUM makes their space reusable but doesn't shrink the files. After the cleanup, the indexes
  are still 6.4 GB instead of 3.9 GB, and Q1 and Q6 stay 10–20% slower than the baseline.
- The ClickHouse Q6 times are under a second and vary by ±0.1 s between phases. Those
  differences are noise.

### Key findings (writes)

1. **Postgres is the clear winner for single corrections.**
   - A one-line update takes 7 ms and a one-order delete 6 ms, at every scale.
   - Small batches (100 rows) take about 0.2 s.
   - Each run is one atomic transaction.
2. **ClickHouse lightweight DELETE and UPDATE cost scales with the table size, not with the
   batch.**
   - At SF 10, deleting a single order takes 0.9 s median and 6.1 s p95.
   - Deleting 1,000 orders takes 19 s and 53 s of CPU per run, with all three cores busy. A
     lightweight delete starts a mutation on every part of the table, and each part has to check
     the condition.
   - A 10,000-line update takes 23 s and 66 s of CPU per run.
   - They grew 2–5x from SF 1 to SF 10. **At 200 GB (about 20 times SF 10), a steady stream of
     corrections would take a large share of the server's CPU.** This method is a poor fit for
     frequent corrections.
3. **ReplacingMergeTree makes every correction cheap, and moves the cost to reads.**
   - Every update, delete and upsert takes 0.03–0.3 s, at every scale.
   - **It is the most efficient method for writes, but the cost is slower reads.** Every read
     needs `FINAL`: Q1 takes 5.6 s instead of 2.8 s, and Q6 0.8 s instead of 0.36 s. That is
     still 6.5x faster than Postgres for Q1. Section 8 measures all 22 queries: reads are about
     1.8x slower on average.
   - The other costs are covered in section 6: `final = 1` is needed permanently, tombstones
     stay, and a correction that changes the sort key would duplicate the row.
4. **Bulk restatements reverse the picture.**
   - Postgres can't restate one year of data at SF 10 within 30 minutes. The time went from 29 s
     at SF 1 to 236 s at SF 3, and all three SF 10 attempts timed out. They wrote 200 GB, then
     rolled back. Their dead rows grew the table by 5.3 GB, and 2.7 GB of that stayed after
     VACUUM.
   - ClickHouse rebuilds the year in a staging table and swaps it in with `REPLACE PARTITION` in
     14–21 s. **That operation is atomic**, so readers never see a half-restated year.
5. **Batch loads strongly favour ClickHouse.**
   - The daily-load scenario (15,000 orders, ~60,000 lines) takes 0.2–0.3 s on ClickHouse.
   - On Postgres it takes 36 s (p95 79 s), because every line updates four indexes on a table
     that no longer fits in memory.
   - Inserting one order per statement is fine on both: about 7 ms per order on Postgres and
     13 ms on ClickHouse. But ClickHouse creates two parts per order, so a high-rate trickle
     needs `async_insert`.
6. **Maintenance is part of the cost on both engines.**
   - Postgres needs VACUUM: 466 s at SF 10 here. Without it, reads slow down (Q1 +36% after the
     writes) and the table grows (14.3 → 19.6 GB).
   - ClickHouse merges in the background. Its storage barely changed, and `OPTIMIZE FINAL` took
     87–125 s.

**For the risk assessment:**
- **A columnar migration only works with a deliberate correction design:** ReplacingMergeTree,
  `final = 1` set in the readers' settings profile
  ([clickhouse/users.d/profile.xml](clickhouse/users.d/profile.xml)), and `REPLACE PARTITION`
  for restatements.
- **The lightweight DELETE and UPDATE that look like a drop-in replacement for SQL UPDATE are the
  slowest option**, and their cost grows with the data.
- **The atomicity gap** (section 6) and **the memory-tracker failure** above both need mitigation
  before go-live.

## 8. Read cost of ReplacingMergeTree

Section 7 recommends ReplacingMergeTree for corrections and measured its read cost only on Q1
and Q6. This section reruns the whole SF 10 read benchmark (section 4) on ReplacingMergeTree,
read with `final = 1`, so the speedup over Postgres can be restated for the design that would
actually go to production.

```bash
python scripts/run_queries.py --engine clickhouse --sf 10 --table-engine replacing
python scripts/run_queries.py --compare --sf 10 --table-engine replacing   # vs Postgres
```

### Setup

- **Tables:** all 8 tables are rebuilt as `ReplacingMergeTree(_version, _is_deleted)` in the
  database `sf10_rmt`, from the `sf10` data, with the same columns, partitions and `ORDER BY` as
  section 2 (the `ORDER BY` key is unique per row on every table). The writes benchmark
  converted only `orders` and `lineitem`. Here the reference tables are converted too, because
  they also receive corrections in production. The build takes 201 s, then the script waits for
  background merges as the loader does.
- **Reads:** every query runs with `final = 1`. The data is fresh: version 0, nothing deleted,
  so all results match Postgres (`--compare`), and the cost measured is `FINAL` itself.
- **Method:** identical to section 4: one cold run after a restart with the data evicted from
  the page cache, then 5 warm runs, with the same server and memory settings.
- **Raw data:** `results/queries_clickhouse_rmt_sf10_20261030T144718Z.json`, saved as engine `clickhouse_rmt` in `results/queries.csv`.

### Results (SF 10)

The speedup is Postgres time ÷ ClickHouse time, computed exactly as in section 4. The same
code reproduces section 4's MergeTree figures.

| Speedup over Postgres | MergeTree (section 4) | ReplacingMergeTree + `FINAL` |
|---|--:|--:|
| Cold, as written (17 of 22) | 28.8x | **17.1x** |
| Warm, as written (17 of 22) | 22.8x | **10.9x** |
| Cold / warm with the tuned Q3, Q5, Q8, Q9 (21 of 22) | 19.5x / 15.6x | **12.5x / 8.6x** |
| Total cold time, best version (Postgres: 2,020 s) | 168 s | 248 s |
| Total warm time, best version (Postgres: 1,722 s) | 152 s | 191 s |
| CPU time, warm, original queries | 99 s | 209 s |
| Queries slower than Postgres when warm (best version) | Q17, Q19 | Q17, Q19, Q22 |
| Failures | Q3, Q5, Q8, Q9, Q21 orig. (per-query memory limit) | the same five |

Per query, ClickHouse times in seconds: median (p95) for warm. Slowdown = ReplacingMergeTree ÷
MergeTree, warm (cold). Memory is the container's peak anonymous memory on the warm runs, MB.

| Query | Version | MergeTree cold | MergeTree warm | Replacing cold | Replacing warm | Slowdown | Memory, MT → RMT |
|---|---|--:|--:|--:|--:|--:|--:|
| Q1 | original | 2.99 | 2.58 (2.86) | 6.49 | 5.78 (5.99) | **2.25x** (2.17x) | 490 → 1,016 |
| Q2 | original | 1.27 | 0.94 (1.07) | 1.65 | 1.33 (1.72) | **1.42x** (1.29x) | 1,403 → 1,428 |
| Q2 | rewritten | 2.17 | 2.00 (2.20) | 2.52 | 2.19 (2.62) | **1.10x** (1.17x) | 1,334 → 1,439 |
| Q3 | original | memory limit | | memory limit | | — | — |
| Q3 | tuned | 9.40 | 9.43 (9.81) | 8.94 | 9.36 (12.05) | **0.99x** (0.95x) | 1,719 → 1,927 |
| Q4 | original | 2.84 | 1.60 (1.75) | 4.32 | 3.39 (3.88) | **2.12x** (1.52x) | 748 → 1,065 |
| Q4 | rewritten | 4.00 | 2.75 (2.88) | 6.17 | 4.79 (4.88) | **1.74x** (1.54x) | 1,478 → 1,848 |
| Q5 | original | memory limit | | memory limit | | — | — |
| Q5 | tuned | 22.48 | 25.03 (29.89) | 19.37 | 24.51 (33.62) | **0.98x** (0.86x) | 2,408 → 2,312 |
| Q6 | original | 0.37 | 0.18 (0.18) | 0.73 | 0.67 (0.94) | **3.68x** (2.00x) | 483 → 626 |
| Q7 | original | 6.74 | 7.33 (8.82) | 11.10 | 11.93 (12.35) | **1.63x** (1.65x) | 2,205 → 2,290 |
| Q8 | original | memory limit | | memory limit | | — | — |
| Q8 | tuned | 35.04 | 28.14 (28.92) | 40.52 | 25.55 (38.42) | **0.91x** (1.16x) | 2,525 → 2,556 |
| Q9 | original | memory limit | | memory limit | | — | — |
| Q9 | tuned | 32.02 | 38.12 (40.82) | 54.51 | 38.28 (47.90) | **1.00x** (1.70x) | 2,602 → 2,969 |
| Q10 | original | 6.49 | 5.54 (5.87) | 9.20 | 8.67 (8.74) | **1.56x** (1.42x) | 2,662 → 2,648 |
| Q11 | original | 0.97 | 0.77 (0.81) | 1.57 | 1.13 (1.14) | **1.47x** (1.62x) | 657 → 661 |
| Q12 | original | 1.98 | 0.60 (0.78) | 3.19 | 2.59 (2.78) | **4.31x** (1.62x) | 668 → 2,224 |
| Q13 | original | 12.35 | 2.62 (2.76) | 13.73 | 4.93 (5.21) | **1.88x** (1.11x) | 1,142 → 2,179 |
| Q14 | original | 0.26 | 0.19 (0.21) | 0.35 | 0.20 (0.21) | **1.06x** (1.32x) | 738 → 718 |
| Q15 | original | 0.25 | 0.24 (0.34) | 0.45 | 0.36 (0.36) | **1.50x** (1.82x) | 659 → 618 |
| Q16 | original | 0.64 | 0.41 (0.42) | 1.05 | 0.63 (0.68) | **1.54x** (1.63x) | 690 → 688 |
| Q17 | original | 4.02 | 3.58 (3.88) | 12.98 | 13.56 (14.78) | **3.78x** (3.23x) | 1,023 → 1,870 |
| Q17 | rewritten | 2.51 | 1.54 (1.82) | 6.67 | 6.50 (7.01) | **4.21x** (2.66x) | 566 → 935 |
| Q18 | original | 4.90 | 4.69 (6.45) | 12.27 | 11.52 (12.71) | **2.46x** (2.51x) | 1,635 → 2,064 |
| Q19 | original | 3.41 | 1.41 (1.43) | 5.65 | 4.30 (5.10) | **3.05x** (1.66x) | 774 → 1,091 |
| Q20 | original | 0.82 | 0.28 (0.29) | 1.23 | 0.86 (0.87) | **3.11x** (1.50x) | 520 → 567 |
| Q20 | rewritten | 0.87 | 0.43 (0.43) | 1.36 | 0.98 (1.02) | **2.27x** (1.56x) | 570 → 572 |
| Q21 | original | memory limit | | memory limit | | — | — |
| Q21 | rewritten | 19.49 | 19.89 (20.05) | 43.35 | 27.41 (39.76) | **1.38x** (2.22x) | 1,905 → 1,967 |
| Q22 | original | 0.80 | 0.54 (0.55) | 1.22 | 0.92 (0.98) | **1.70x** (1.53x) | 624 → 775 |
| Q22 | rewritten | 0.66 | 0.49 (0.51) | 1.43 | 1.01 (1.20) | **2.06x** (2.16x) | 726 → 937 |

### Key findings (ReplacingMergeTree reads)

1. **`FINAL` makes reads about 1.8x slower on average (geometric mean, warm; 1.6x cold).** That
   halves the speedup over Postgres: 22.8x → 10.9x warm as written, and 15.6x → **8.6x** with
   the tuned joins, **below the 10x claim**. Cold, it stays above: 17.1x and 12.5x.
2. **The cost is largest where the scan is the whole query.** `FINAL` merges the rows of
   overlapping parts by the sort key before the query sees them. Q6, Q12, Q17, Q19 and Q20, which
   are mostly a filtered scan of `lineitem`, are 3–4.3x slower. The heavy joins (tuned Q3, Q5,
   Q8, Q9) are unchanged (0.91–1.00x warm), because the join, not the scan, dominates.
3. **The penalty is a constant factor, not one that grows with scale.** In the writes benchmark
   (section 7), Q1 was 2.4x, 2.0x and 2.0x slower than MergeTree at SF 1, 3 and 10. So the 200
   GB system should expect the same roughly 2x on reads, not worse.
4. **Memory rises, but no new failures at SF 10.** The same five queries fail, all on the
   per-query limit. Some queries need much more memory: Q12 668 → 2,224 MB, Q13 1,142 → 2,179 MB,
   Q1 490 → 1,016 MB. On a server sized as tightly as this one, that is less headroom for
   concurrency.
5. **Merging doesn't remove the cost.** After the build, `lineitem` settled at 42 parts against
   12 for MergeTree. But section 7's Q1 was no faster after `OPTIMIZE FINAL` (6.53 s against
   5.56 s before). A production table that receives corrections all day will have more
   overlapping parts, not fewer, so these figures are closer to a best case.

**For the risk assessment and TCO:**
- **The 10x claim holds on warm reads only for plain MergeTree.** With the correction design that
  section 7 requires, the warm speedup at SF 10 is 8.6–10.9x, and cold 12.5–17.1x. The claim
  should be restated as "about 10x", conditional on the correction design.
- **CPU per warm query doubled (99 → 209 s for the 17 originals).** The TCO should size
  ClickHouse compute for ReplacingMergeTree, not for the MergeTree benchmark.
- **Lightweight-UPDATE tables hold about 1 GB of server memory while idle.** If lightweight
  UPDATE is used at all, for example for rare fixes, this has to be in the memory sizing.

## 9. Concurrency

Sections 4–8 run one query at a time on an idle server. A reporting warehouse doesn't work
that way: several reports run at once while corrections keep arriving. This section runs the
TPC-H throughput test at SF 10 on both engines, with corrections applied during the reads.

```bash
python scripts/run_concurrency.py --engine postgres --sf 10 --stop-if-flat
python scripts/run_concurrency.py --engine clickhouse --sf 10
python scripts/run_concurrency.py --engine clickhouse --sf 1 --levels 1,2 --queries q01,q06  # smoke test
```

### Setup

A **stream** is one analyst running the full set of 22 reports, one after another. The number
of streams S is the number of analysts working at the same time.
[scripts/run_concurrency.py](scripts/run_concurrency.py) runs one level per value of S
(1, 2, 4, 8). The levels run one after another, never at the same time, and each one runs as
follows:

1. **Prepare:** rebuild fresh copies of `orders` and `lineitem`, so every level starts from
   the same data, then restart the container with an empty cache (cold start).
2. **Start all streams at once and start the clock:**
   - **S query streams**, each running all 22 TPC-H queries in sequence. Every stream runs the
     same 22 queries, but in a different order, so the streams don't all hit the same heavy
     query at the same moment.
   - **1 corrections stream**, applying S correction sets in sequence (inserts, updates,
     deletes, upserts): late-arriving corrections while the reports run.
3. **Stop the clock** when every stream has finished. The elapsed time is the level's
   **wall time**.

| Level (S) | Query streams in parallel | Queries in total | Correction sets |
|--:|--:|--:|--:|
| 1 | 1 | 22 | 1 |
| 2 | 2 | 44 | 2 |
| 4 | 4 | 88 | 4 |
| 8 | 8 | 176 | 8 |

For example, Postgres with 2 streams took 4,766 s: two analysts each ran all 22 reports side
by side, corrections ran alongside them, and the last report finished 4,766 s after the start.

The details of each step:

- **Data:** each level starts from fresh copies of `orders` and `lineitem`, built as in the
  write benchmark (section 7), so every level sees the same data.
  - Postgres: schema `c_sf10`, with `VACUUM ANALYZE` after the build and autovacuum on, as in
    production. The other 6 tables are read from `sf10` through `search_path`.
  - ClickHouse: database `c_sf10_rmt`, with all 8 tables as ReplacingMergeTree (section 8).
- **Cold start:** the container is stopped, its data is evicted from the page cache, and it is
  restarted, as for the cold runs in section 4.
- **Query streams:** S streams start together. Stream *i* runs the 22 queries once, in the
  order of TPC-H stream *i* (spec Appendix A). Each query uses its best version for that engine:
  the fastest one that completed in the single-user SF 10 run. On ClickHouse that is the
  ReplacingMergeTree run with `final = 1` (section 8), including the tuned Q3, Q5, Q8 and Q9.
- **Corrections stream:** one more stream applies S correction sets back to back, as TPC-H
  applies S refresh pairs.
  - **Which corrections:** the write scenarios from section 7, with the same pre-generated rows
    and the same code: `insert_batch`, `insert_small`, `update_point`, `update_100`,
    `update_1k`, `update_10k`, `delete_point`, `delete_100`, `delete_1k`, `upsert_1k`. The
    restatement is left out: it is a scheduled bulk job, not a late correction, and a Postgres
    timeout at SF 10.
  - **How many times:** a set is one run of each of the 10 scenarios, so each scenario runs
    once at level 1, twice at level 2, 4 times at level 4 and 8 times at level 8 (10, 20, 40
    and 80 write operations).
  - **Each repeat uses different rows:** set *k* uses run *k* of each scenario from section 7.
    For example, at level 4 `update_1k` runs 4 times, on 4 different batches of 1,000 line
    items. No row is corrected twice.
  - **Set by set, one at a time:** the stream applies the 10 scenarios in the order above, then
    starts the next set. Only one correction runs at a time, alongside the query streams.
  - **Method:** each engine uses its production method: Postgres transactions, ClickHouse
    ReplacingMergeTree inserts (the lightweight `UPDATE`/`DELETE` method from section 7 is not
    used here).
- **Limits:** the same as every other section: 3 CPUs and 4 GB per container, a 30-minute
  timeout, and on ClickHouse `max_memory_usage` = 3 GB per query and
  `max_server_memory_usage` = 3.5 GB for the server (section 2). Timeouts and memory-limit
  errors are recorded as results. A failed query is not retried, and its stream moves on.
- **Metrics:**
  - each query's time and status;
  - its **slowdown**: time ÷ the single-user warm median;
  - **queries per hour**: the level's throughput, i.e. the rate of successful work. It is the
    number of completed queries ÷ the level's wall time × 3600.
    - *Completed queries* are those from the query streams only. Failed and timed-out queries
      are left out, and so are corrections, which are reported separately.
    - *Wall time* (step 3 above) ends when the last query finishes. At every level the
      corrections finished earlier, within the first 3–62 s on ClickHouse and 94–409 s on
      Postgres.
    - Example: Postgres with 2 streams completed 44 queries in 4,766 s, so
      44 ÷ 4,766 × 3600 = 33.2 queries per hour;
  - the corrections' times and failures;
  - the container's CPU, peak memory and disk reads over the level (cgroup, as in section 4).
- **Postgres with 8 streams was not run.** The run was interrupted after about 2.5 hours, when
  the runner process died, and was not repeated. At 4 streams Postgres was still far below
  ClickHouse's throughput and already timing out, so 8 streams would not change the
  conclusions.
- **Raw data:** `results/concurrency_postgres_sf10_20261031T135127Z.json`,
  `results/concurrency_clickhouse_sf10_20261031T222440Z.json`, and one line per level in
  `results/concurrency.csv`.

### Results (SF 10)

**How to read this table.** Each row is one level: S streams running at the same time, plus
the corrections stream.
- **Queries/h** is the throughput: completed queries per hour. Read it down each engine: if it
  rises, adding analysts gets more reports done; if it stays flat, the server is at its limit.
  Every query counts as 1, whether it is a light or a heavy one.
- **Work completed** weighs each completed query by its single-user warm time. It is the share
  of the reporting workload that actually produced a result. Read it together with queries/h:
  queries/h alone flatters ClickHouse at high concurrency, because the queries that still
  complete there are mostly the light ones.
- **Slowdown** is a query's time here ÷ its single-user warm time: 1x is as fast as alone, 3x is
  3 times slower. The **geomean** is the typical slowdown across the completed queries (a
  geometric mean, so a few extreme ratios don't dominate). The **p95** is the bad end: 95% of
  the completed queries slowed down by at most this much. Failed queries are not included, so
  on ClickHouse from 2 streams on they describe the queries that survived.

| Streams | Engine | Wall time | Completed | Failed | Queries/h | Work completed | Slowdown, geomean | Slowdown, p95 | Avg cores | Disk read |
|--:|---|--:|--:|---|--:|--:|--:|--:|--:|--:|
| 1 | Postgres | 4,133 s | 21 / 22 | 1 timeout | 18.3 | 98% | 2.29x | 21.1x | 0.48 | 254 GB |
| 1 | ClickHouse | 223 s | 21 / 22 | 1 memory | 338.9 | 80% | 1.45x | 2.8x | 2.53 | 9.8 GB |
| 2 | Postgres | 4,766 s | 44 / 44 | — | 33.2 | 100% | 3.51x | 79.0x | 0.67 | 553 GB |
| 2 | ClickHouse | 505 s | 36 / 44 | 8 memory | 256.7 | 62% | 3.32x | 6.5x | 2.41 | 22.6 GB |
| 4 | Postgres | 6,796 s | 82 / 88 | 6 timeouts | 43.4 | 86% | 4.02x | 82.5x | 1.10 | 929 GB |
| 4 | ClickHouse | 816 s | 61 / 88 | 26 memory, 1 timeout | 269.0 | 34% | 7.74x | 20.6x | 2.56 | 39.4 GB |
| 8 | Postgres | not run | | | | | | | | |
| 8 | ClickHouse | 894 s | 68 / 176 | 108 memory | 273.7 | **9%** | 16.61x | 36.2x | 2.78 | 37.9 GB |

ClickHouse's container peaked at its 4,096 MB limit at every level, and Postgres's at 4 streams.
Postgres's peak working memory stayed under 1 GB. The rest was page cache.

Corrections applied during the reads (S sets per level, 10 runs per set):

| Streams | Postgres: time per set | Postgres: failed | ClickHouse: time per set | ClickHouse: failed |
|--:|--:|--:|--:|--:|
| 1 | 94 s | 0 / 10 | 3.1 s | 0 / 10 |
| 2 | 112 s | 0 / 20 | 5.4 s | 0 / 20 |
| 4 | 102 s | 0 / 40 | 5.5 s | **4 / 40** |
| 8 | not run | | 7.8 s | **28 / 80** |

The ClickHouse corrections that failed are of every kind: small inserts, point and batch
updates, deletes, upserts and the daily batch insert. All of them ran out of memory (code 241,
or code 33 with an Arrow allocation error while reading the inserted batch).

ClickHouse queries that failed, out of S runs per level (blank: none failed):

| Query | 1 stream | 2 streams | 4 streams | 8 streams |
|---|--:|--:|--:|--:|
| Q9 (tuned) | 1 / 1 | 2 / 2 | 4 / 4 | 8 / 8 |
| Q7 | | 2 / 2 | 4 / 4 | 8 / 8 |
| Q8 (tuned) | | | 4 / 4 | 8 / 8 |
| Q10 | | 1 / 2 | 4 / 4 | 8 / 8 |
| Q21 (rewritten) | | | 3 / 4 | 8 / 8 |
| Q5 (tuned) | | 1 / 2 | 2 / 4 | 8 / 8 |
| Q13 | | 2 / 2 | 1 / 4 | 7 / 8 |
| Q1, Q4, Q12, Q18 | | | Q4 1, Q12 2, Q18 1 | 7 / 8 each |
| Q2, Q3 (tuned), Q17 (rewritten) | | | Q3 1 | 6, 6, 5 / 8 |
| Q11, Q22, Q6, Q15, Q20 | | | | 3, 2, 1, 1, 1 / 8 |
| Q14, Q16, Q19 | | | | |

Postgres timeouts: Q13 in 5 of 7 runs (including the 1-stream run), Q9 and Q21 once each at
4 streams.

### Key findings (concurrency)

1. **With one report at a time, ClickHouse's lead holds: 18.5x more completed queries per
   hour.** The whole stream takes 223 s against 4,133 s, and the corrections stream barely
   slows it down (1.45x geomean). But even here Q9 runs out of memory, which it didn't do alone
   in section 8. The corrections stream's memory is enough to push it over.
2. **From 2 streams on, ClickHouse's throughput stops growing and its useful work collapses.**
   Queries per hour stay at 257–274. The share of the workload that completes falls from 80%
   to 62%, 34% and 9% at 8 streams.
   - The failures are memory, not time. Each query fits the 3 GB per-query limit on its own, but
     together they exceed the 3.5 GB server limit, and whichever query asks for the next block
     fails. Failures are quick: 15–40 s on average.
   - The heavy joins fail first: Q7 and Q9 never complete with 2 or more streams; Q5, Q8, Q10
     and Q21 also fail every time at 8 streams. These multi-table joins are the closest match to
     the regulatory reports in section 1.
   - CPU is close to saturated at every level (2.4–2.8 of 3 cores). So even with more memory,
     the completed queries would slow down roughly in proportion to the number of streams.
3. **Postgres degrades slowly, but from a much slower start.** Its completed queries per hour
   rise from 18.3 to 33.2 and 43.4. It completes 86–100% of the workload, and it never uses more
   than 1.1 of its 3 cores.
   - It is I/O-bound. One stream reads 254 GB from disk for a 16.7 GB database, and 4 streams
     929 GB. The 4 GB container's page cache can't hold the working set of several concurrent
     scans.
   - The queries that suffer most are the fast index lookups: Q2, Q11, Q17 and Q19 run 15–100x
     slower than alone, because the scans of the other streams evict their index pages. The
     large scans mostly slow 1–3x.
   - Q13 times out in 5 of 7 runs, even with a single stream, but it took 42 s alone (warm) and
     115 s cold. It may be choosing a different plan on the `c_sf10` copies, but the plan wasn't
     checked here. **This is an open item.**
4. **Under load, ClickHouse loses corrections; Postgres doesn't.** With 4 and 8 streams, 32 of
   120 ClickHouse correction runs failed on memory. Postgres applied all 70 of its runs, at a
   steady 94–112 s per set (most of it the daily batch insert and the 10,000-line update, as in
   section 7). When they succeed, ClickHouse's
   corrections are still 19–30x faster per set.
5. **The comparison at equal hardware ends in different failure modes.** With 4 streams,
   Postgres finishes the batch in 1.9 hours, with 6 reports timed out. ClickHouse finishes in
   14 minutes, with 27 reports and 4 correction runs failed. Neither result is fit for a
   regulatory window as sized here. The difference is that Postgres needs more I/O and memory
   to get faster, while ClickHouse needs more memory to give complete answers at all.

**For the risk assessment and TCO:**
- **The 10x claim is a single-user figure.** At 2–4 concurrent reports on this hardware,
  ClickHouse completes 7.7x and 6.2x more queries per hour than Postgres. Weighted by work
  done, it completes less of the workload than Postgres from 2 streams on.
- **Memory, not CPU, sizes a ClickHouse server for concurrent reports.** The TCO should size
  memory as (concurrent heavy reports × per-query peak, about 2–3 GB each at SF 10, section 8)
  plus headroom for inserts. Per-query memory grows with the data, so at 200 GB that figure will
  be far larger than at SF 10. The alternative is a hard limit on concurrent reports, which
  stretches the window.
- **Admission control is required before go-live:** `max_concurrent_queries` or workload
  scheduling for reports, and a separate memory budget for the correction writes, so that a
  read peak can't reject a correction.
- **Every correction writer needs retry logic on ClickHouse.** A rejected write is silently
  missing from the next report unless the loader retries it. Postgres never rejected one.
- **Postgres sizing is I/O and page cache.** More RAM for cache and faster storage would help
  it more than more cores, since it used at most 1.1 of its 3.

## 10. TCO comparison

[scripts/tco.py](scripts/tco.py) prices both options over **3 years**, self-managed on AWS
(us-east-1, EC2 on-demand list prices), at production scale (SF 200, ~200 GB). Every price,
rate and benchmark figure it uses is in [tco/assumptions.toml](tco/assumptions.toml), with its
source and the date it was read. Assumptions that aren't measured or published are marked as
such and varied in the sensitivity table.

```bash
python scripts/tco.py
```

### How each option is sized

Both options run a primary and an HA replica of the same size, with gp3 volumes and snapshots.

- **Postgres** (status quo) meets its 6–8 h window by definition, so its node only has to keep
  the benchmark's resources per GB of data: memory × 20 (80 GB) and its peak cores under load
  × 20 (22 vCPU). That's an **r7i.8xlarge** (32 vCPU, 256 GB). Storage is 20 × the SF 10 size,
  plus the 37% growth measured after the write workload: 458 GB.
- **ClickHouse** is sized by **memory**, because memory is what made it fail (section 9), while
  CPU only set the speed. It needs 4 concurrent reports × a 60 GB per-query peak (3 GB at
  SF 10, scaled with the data, the ratio that was tested), plus the server and inserts, ÷ the
  90% of RAM the server may use: **278 GB**, so an **r7i.12xlarge** (48 vCPU, 384 GB). Its data
  is 80 GB.
- **Operations:** 0.5 FTE on both, at the BLS median salary for database administrators,
  loaded by the BLS employer cost of benefits (wages are 70% of compensation): $75k per year.
  There's no ClickHouse premium in the base case: ClickHouse Inc. posts the same pay band for
  its ClickHouse and Postgres infrastructure engineers.
- **Migration** (ClickHouse only, one-off), at the BLS median salary for database architects:
  - 100 regulatory reports × 2 days to port, set `final = 1`, review decimal arithmetic and
    validate against Postgres;
  - plus 3 days for each report that needs a fix. In the benchmark that was 6 of 22 (27%):
    the tuned Q3, Q5, Q8 and Q9, the rewritten Q21, and Q8's decimal precision. As a check, the
    join tuning of 4 queries took about one working day in this project (git history), before
    any production review or sign-off;
  - 10 weeks for the correction pipeline: ReplacingMergeTree tables, versioned writes, retries,
    admission control and `REPLACE PARTITION` restatements;
  - a 3-month parallel run, with Postgres still in production and 0.5 FTE reconciling results.

### Results (base case)

| | Postgres | ClickHouse |
|---|--:|--:|
| Node (× 2: primary + replica) | r7i.8xlarge (32 vCPU, 256 GB) | r7i.12xlarge (48 vCPU, 384 GB) |
| Compute per year | $37k | $56k |
| Storage + snapshots per year | $4k | $1k |
| Operations per year | $75k | $75k |
| **Running cost per year** | **$116k** | **$131k** |
| Migration: reports + pipeline (332 days) | — | $301k |
| Migration: parallel run | — | $48k |
| **3-year TCO** | **$348k** | **$742k** |

**ClickHouse costs 113% more over 3 years, not 60% less.** Its infrastructure costs 37% more
per year, and the migration alone costs as much as 3 years of running Postgres.

### Sensitivity

One assumption changed at a time. "Licence for a 60% saving" is how much the current
warehouse would have to cost per year in licences and support, on top of the Postgres
figures, for ClickHouse to cost 60% less over 3 years.

| Case | ClickHouse node | Postgres, 3 y | ClickHouse, 3 y | ClickHouse vs Postgres | Infra per year | Licence for a 60% saving |
|---|---|--:|--:|--:|--:|--:|
| Base case | r7i.12xlarge | $348k | $742k | +113% | +37% | $502k/y |
| 1 concurrent report (base 4) | r7i.4xlarge | $348k | $631k | +81% | -53% | $410k/y |
| 2 concurrent reports | r7i.8xlarge | $348k | $686k | +97% | -8% | $456k/y |
| 8 concurrent reports | r7i.24xlarge | $348k | $909k | +161% | +172% | $641k/y |
| Per-query memory grows 25% as fast as the data (more spilling, untested) | r7i.4xlarge | $348k | $631k | +81% | -53% | $410k/y |
| 3-year reserved instances, both engines | r7i.12xlarge | $287k | $646k | +125% | +25% | $442k/y |
| Postgres needs r7i.16xlarge for I/O | r7i.12xlarge | $459k | $751k | +64% | -28% | $473k/y |
| 50 reports to migrate (base 100) | r7i.12xlarge | $348k | $614k | +77% | +37% | $396k/y |
| 200 reports to migrate | r7i.12xlarge | $348k | $997k | +187% | +37% | $715k/y |
| ClickHouse operators cost 25% more (scarcity) | r7i.12xlarge | $348k | $798k | +129% | +37% | $549k/y |
| ClickHouse needs 0.75 FTE to operate (base 0.5) | r7i.12xlarge | $348k | $854k | +146% | +37% | $596k/y |

### Key findings (TCO)

1. **At 200 GB, people cost more than machines.** Operations and migration are 65% of
   Postgres's 3-year cost and 77% of ClickHouse's. Infrastructure differences of tens of thousands of dollars a
   year can't pay back a migration of about $350k within 3 years.
2. **ClickHouse's infrastructure is only cheaper at low concurrency.** With 1 report at a
   time, or if per-query memory could be capped by spilling (untested), it needs a 128 GB node
   and costs 53% less to run, close to the claimed 60%. At 4 concurrent reports, memory makes
   it 37% more expensive, and at 8, 172% more.
3. **"10x faster" and "60% cheaper" can't both come from the hardware.** A smaller ClickHouse
   node is cheaper but has less memory, which is exactly what makes concurrent reports fail.
4. **The 60% saving needs a licence to remove.** Against self-managed Postgres, no case in the
   table comes close. It's only plausible if the current warehouse is a commercial product
   costing about **$400k–$700k per year** in licences and support. That figure should be
   checked against the company's actual contract before the claim is accepted.
5. **The figures are estimates.** They extrapolate laptop measurements at SF 10 to SF 200,
   assuming that 20 × the data with 20 × the resources behaves like the benchmark. That holds
   best for ClickHouse's memory and least for Postgres's I/O, which is why the sensitivity
   table includes a twice-as-large Postgres node.

## 11. Risk assessment

Each risk is rated by likelihood and impact for a regulatory-reporting system, with the
section that found it. The first table covers migrating; the second covers staying on
Postgres, since not migrating has risks too.

### Risks of migrating to ClickHouse

| # | Risk | Evidence | Likelihood | Impact | Mitigation |
|--:|---|---|---|---|---|
| 1 | **Reports and corrections fail on memory under concurrency.** Failed reports are missing from the window; rejected corrections are silently absent from the next report. | Work completed 80% → 9% at 1 → 8 streams; 32 of 120 corrections rejected at 4–8 streams (9) | High | High | Size memory for concurrency (section 10); admission control (`max_concurrent_queries`, workload scheduling); a separate memory budget for writes; retries on every writer |
| 2 | **Heavy joins need per-report tuning, and it has to be redone as data grows.** No single server setting runs all reports. | Q3, Q5, Q8, Q9 out of memory at SF 10 with the default join; the fix makes them 1.1–1.8x slower at small scales (4, 5) | High | High | Inventory of heavy reports, per-query `join_algorithm`, regression tests at production volume before every growth step |
| 3 | **Silent decimal precision loss in financial figures.** | Q8 division truncated to 4 digits; a Float64 literal truncated 200,000 restated tax rates with no error (4, 7) | High | High | Review all decimal arithmetic, use explicit `toDecimal`; automated reconciliation against Postgres in the parallel run |
| 4 | **No multi-statement atomicity.** A two-table correction (lines and order total) can be read half-applied. | Each Postgres run is one transaction; on ClickHouse a delete-then-insert or two-table change is several statements (6, 7) | High | High | Single-table versioned inserts; `REPLACE PARTITION` for bulk changes (atomic); reconciliation checks before reports run |
| 5 | **The correction design is a one-way constraint.** Lightweight `UPDATE`/`DELETE` cost grows with the table; ReplacingMergeTree needs `final = 1` permanently, keeps tombstones, and duplicates a row whose sort key changes. | Lightweight `delete_1k` 19 s and 53 s CPU per run at SF 10; tombstones survive `OPTIMIZE FINAL` (7, 8) | Certain | Medium | ReplacingMergeTree with `final = 1` in the readers' profile; forbid sort-key corrections (delete + insert instead) |
| 6 | **The speedup is smaller than claimed in production conditions.** | Warm speedup 22.8x → 8.6x with the tuned joins and `FINAL`; at equal hardware, less work completed than Postgres from 2 streams on (4, 8, 9) | Certain | Medium | Restate the target as "about 9–11x, single-user, warm"; accept only after a production-scale proof of concept |
| 7 | **Default memory accounting stops accepting writes.** | Memory-tracker drift rejected one-row inserts at 1.0–1.7 GB real use (7) | Medium | High | `memory_worker_correct_memory_tracker = 1`; alert on tracked vs real memory |
| 8 | **Reports need rewriting, not just moving.** | Correlated Q21 fails under every join setting; its rewrite works (4) | Certain | Medium | Rewrite and re-validate every correlated report; budgeted in section 10 |
| 9 | **The cost case doesn't hold.** | +113% over 3 years against self-managed Postgres (10) | High | High | Check the current licence cost; the saving needs about $400k–$700k a year of it |
| 10 | **New skills for the team** (merges, parts, memory limits, settings profiles). | The tracker drift and join limits are settings an operations team finds only in production (5, 7) | Medium | Medium | Training before the parallel run; vendor support contract |

### Risks of staying on Postgres

| # | Risk | Evidence | Likelihood | Impact | Mitigation |
|--:|---|---|---|---|---|
| 1 | **Bulk restatements don't finish.** One year couldn't be restated in 30 minutes at SF 10, and the attempts left 2.7 GB of bloat. | 3 timeouts, 200 GB written and rolled back (7) | High | High | Restate in partitions (declarative partitioning by year) or in batches; schedule outside the window |
| 2 | **The window grows with data and concurrency.** Postgres is I/O-bound and its fast lookups suffer most when reports run together. | ≤ 1.1 of 3 cores used; Q2, Q11, Q17, Q19 15–100x slower under load (9) | High | Medium | More RAM for cache and faster storage (not more cores); partitioning; fewer concurrent heavy reports |
| 3 | **Unexplained timeouts.** | Q13 timed out in 5 of 7 concurrent runs but took 42 s alone; the Q4 rewrite is unstable (4, 9) | Medium | Medium | Check the plans (`EXPLAIN ANALYZE`) on production copies; pin statistics targets |
| 4 | **Maintenance cost.** | VACUUM 466 s at SF 10; the indexes stay 64% larger after it (7) | Certain | Low | Autovacuum tuning; periodic `REINDEX CONCURRENTLY` |

### Overall assessment

- **The 10x claim is about right for single reports, and not for the system as a whole.**
  ClickHouse is 8.6–10.9x faster on single reports (warm) in the design that corrections
  require, but at equal hardware it delivers less of a concurrent workload than Postgres.
- **The 60% cost claim isn't supported** against a self-managed Postgres baseline. It depends
  on removing a licence the benchmark didn't price.
- **The highest risks are the ones that produce wrong or missing numbers silently**: rejected
  corrections (1), decimal truncation (3) and half-applied corrections (4). For a regulator,
  these matter more than speed. All three can be mitigated, but only with deliberate design
  and a parallel run that reconciles every report against Postgres.
- **Postgres's risks are mostly fixable in place** (partitioned restatements, more cache, plan
  checks) at a fraction of the migration cost. That option should be priced before deciding.
