-- TPC-H Q8. Source: DuckDB tpch extension (TPC-H validation parameters).
-- Tuned variant: same SQL as ../q08.sql plus a per-query join algorithm. With the
-- default hash join this query exceeds the 3 GB per-query memory cap at SF10 (the
-- in-memory hash table on orders/lineitem does not fit). full_sorting_merge sorts
-- both join sides with external (on-disk) sort, so it spills instead of failing.
-- Chosen per query, not in the profile: as a global setting it slows other queries
-- up to 18x (Q17 rewrite), and the global 'auto' alternatives either slow joins that
-- fit in memory or hit the server-wide limit (see README, "ClickHouse join tuning").
-- Known difference (kept as a finding, query left as written): ClickHouse Decimal
-- division keeps the dividend's scale, so mkt_share is truncated to 4 digits
-- (0.0344 vs Postgres 0.0344358904... at SF1). Within the TPC-H answer tolerance,
-- but a silent precision loss for financial ratios after a migration.
SELECT
    o_year,
    sum(
        CASE WHEN nation = 'BRAZIL' THEN
            volume
        ELSE
            0
        END) / sum(volume) AS mkt_share
FROM (
    SELECT
        extract(year FROM o_orderdate) AS o_year,
        l_extendedprice * (1 - l_discount) AS volume,
        n2.n_name AS nation
    FROM
        part,
        supplier,
        lineitem,
        orders,
        customer,
        nation n1,
        nation n2,
        region
    WHERE
        p_partkey = l_partkey
        AND s_suppkey = l_suppkey
        AND l_orderkey = o_orderkey
        AND o_custkey = c_custkey
        AND c_nationkey = n1.n_nationkey
        AND n1.n_regionkey = r_regionkey
        AND r_name = 'AMERICA'
        AND s_nationkey = n2.n_nationkey
        AND o_orderdate BETWEEN CAST('1995-01-01' AS date)
        AND CAST('1996-12-31' AS date)
        AND p_type = 'ECONOMY ANODIZED STEEL') AS all_nations
GROUP BY
    o_year
ORDER BY
    o_year
SETTINGS join_algorithm = 'full_sorting_merge';
