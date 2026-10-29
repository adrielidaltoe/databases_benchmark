-- TPC-H Q9. Source: DuckDB tpch extension (TPC-H validation parameters).
-- Tuned variant: same SQL as ../q09.sql plus a per-query join algorithm. With the
-- default hash join this query exceeds the 3 GB per-query memory cap at SF10 (the
-- in-memory hash table on orders/lineitem does not fit). full_sorting_merge sorts
-- both join sides with external (on-disk) sort, so it spills instead of failing.
-- Chosen per query, not in the profile: as a global setting it slows other queries
-- up to 18x (Q17 rewrite), and the global 'auto' alternatives either slow joins that
-- fit in memory or hit the server-wide limit (see README, "ClickHouse join tuning").
SELECT
    nation,
    o_year,
    sum(amount) AS sum_profit
FROM (
    SELECT
        n_name AS nation,
        extract(year FROM o_orderdate) AS o_year,
        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
    FROM
        part,
        supplier,
        lineitem,
        partsupp,
        orders,
        nation
    WHERE
        s_suppkey = l_suppkey
        AND ps_suppkey = l_suppkey
        AND ps_partkey = l_partkey
        AND p_partkey = l_partkey
        AND o_orderkey = l_orderkey
        AND s_nationkey = n_nationkey
        AND p_name LIKE '%green%') AS profit
GROUP BY
    nation,
    o_year
ORDER BY
    nation,
    o_year DESC
SETTINGS join_algorithm = 'full_sorting_merge';
