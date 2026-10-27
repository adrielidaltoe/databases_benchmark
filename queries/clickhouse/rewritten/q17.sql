-- TPC-H Q17, decorrelated: the per-part 0.2 * avg(l_quantity) subquery becomes
-- a CTE grouped by part, joined back to lineitem.
WITH target_parts AS (
    SELECT
        p_partkey
    FROM
        part
    WHERE
        p_brand = 'Brand#23'
        AND p_container = 'MED BOX'
),
qty_threshold AS (
    SELECT
        l_partkey AS t_partkey,
        0.2 * avg(l_quantity) AS max_qty
    FROM
        lineitem
        JOIN target_parts ON p_partkey = l_partkey
    GROUP BY
        l_partkey
)
SELECT
    sum(l_extendedprice) / 7.0 AS avg_yearly
FROM
    lineitem
    JOIN qty_threshold ON t_partkey = l_partkey
WHERE
    l_quantity < max_qty;
