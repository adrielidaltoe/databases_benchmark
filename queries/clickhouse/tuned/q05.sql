-- TPC-H Q5. Source: DuckDB tpch extension (TPC-H validation parameters).
-- Tuned variant: same SQL as ../q05.sql plus a per-query join algorithm. With the
-- default hash join this query exceeds the 3 GB per-query memory cap at SF10 (the
-- in-memory hash table on orders/lineitem does not fit). full_sorting_merge sorts
-- both join sides with external (on-disk) sort, so it spills instead of failing.
-- Chosen per query, not in the profile: as a global setting it slows other queries
-- up to 18x (Q17 rewrite), and the global 'auto' alternatives either slow joins that
-- fit in memory or hit the server-wide limit (see README, "ClickHouse join tuning").
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= CAST('1994-01-01' AS date)
    AND o_orderdate < CAST('1995-01-01' AS date)
GROUP BY
    n_name
ORDER BY
    revenue DESC
SETTINGS join_algorithm = 'full_sorting_merge';
