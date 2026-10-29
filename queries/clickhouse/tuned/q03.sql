-- TPC-H Q3. Source: DuckDB tpch extension (TPC-H validation parameters).
-- Tuned variant: same SQL as ../q03.sql plus a per-query join algorithm. With the
-- default hash join this query exceeds the 3 GB per-query memory cap at SF10 (the
-- in-memory hash table on orders/lineitem does not fit). full_sorting_merge sorts
-- both join sides with external (on-disk) sort, so it spills instead of failing.
-- Chosen per query, not in the profile: as a global setting it slows other queries
-- up to 18x (Q17 rewrite), and the global 'auto' alternatives either slow joins that
-- fit in memory or hit the server-wide limit (see README, "ClickHouse join tuning").
SELECT
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM
    customer,
    orders,
    lineitem
WHERE
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < CAST('1995-03-15' AS date)
    AND l_shipdate > CAST('1995-03-15' AS date)
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC,
    o_orderdate
LIMIT 10
SETTINGS join_algorithm = 'full_sorting_merge';
