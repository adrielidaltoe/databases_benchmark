-- TPC-H Q4, decorrelated: EXISTS (late lineitem for this order) becomes a CTE
-- of distinct late order keys, inner-joined to orders (semi-join).
WITH late_orders AS (
    SELECT DISTINCT
        l_orderkey
    FROM
        lineitem
    WHERE
        l_commitdate < l_receiptdate
)
SELECT
    o_orderpriority,
    count(*) AS order_count
FROM
    orders
    JOIN late_orders ON l_orderkey = o_orderkey
WHERE
    o_orderdate >= CAST('1993-07-01' AS date)
    AND o_orderdate < CAST('1993-10-01' AS date)
GROUP BY
    o_orderpriority
ORDER BY
    o_orderpriority;
