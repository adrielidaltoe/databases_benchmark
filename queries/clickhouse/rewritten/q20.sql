-- TPC-H Q20, decorrelated: the per-(part, supplier) 0.5 * sum(l_quantity)
-- subquery becomes a CTE grouped by (partkey, suppkey), restricted to the
-- forest parts (the only ones the original evaluates it for). The inner join also
-- drops pairs with no 1994 shipments, as the original's NULL comparison does.
WITH forest_parts AS (
    SELECT
        p_partkey
    FROM
        part
    WHERE
        p_name LIKE 'forest%'
),
shipped_1994 AS (
    SELECT
        l_partkey,
        l_suppkey,
        0.5 * sum(l_quantity) AS half_qty
    FROM
        lineitem
        JOIN forest_parts ON p_partkey = l_partkey
    WHERE
        l_shipdate >= CAST('1994-01-01' AS date)
        AND l_shipdate < CAST('1995-01-01' AS date)
    GROUP BY
        l_partkey,
        l_suppkey
),
excess_suppliers AS (
    SELECT DISTINCT
        ps_suppkey
    FROM
        partsupp
        JOIN shipped_1994 ON l_partkey = ps_partkey
            AND l_suppkey = ps_suppkey
    WHERE
        ps_availqty > half_qty
)
SELECT
    s_name,
    s_address
FROM
    supplier
    JOIN excess_suppliers ON ps_suppkey = s_suppkey
    JOIN nation ON n_nationkey = s_nationkey
WHERE
    n_name = 'CANADA'
ORDER BY
    s_name;
