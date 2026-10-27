-- TPC-H Q13. Source: DuckDB tpch extension (TPC-H validation parameters).
SELECT
    c_count,
    count(*) AS custdist
FROM (
    SELECT
        c_custkey,
        count(o_orderkey)
    FROM
        customer
    LEFT OUTER JOIN orders ON c_custkey = o_custkey
    AND o_comment NOT LIKE '%special%requests%'
GROUP BY
    c_custkey) AS c_orders (c_custkey,
        c_count)
GROUP BY
    c_count
ORDER BY
    custdist DESC,
    c_count DESC
-- ClickHouse default join_use_nulls=0 fills unmatched LEFT JOIN columns with 0, not NULL,
-- so count(o_orderkey) would count order-less customers as 1. Force SQL semantics.
SETTINGS join_use_nulls = 1;
