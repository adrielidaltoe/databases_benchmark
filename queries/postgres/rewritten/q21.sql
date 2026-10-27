-- TPC-H Q21, decorrelated: EXISTS / NOT EXISTS over other lineitems of the same
-- order become one per-order aggregate. For a late line l1:
--   EXISTS other supplier in the order    <=> min(suppkey) <> max(suppkey)
--   NOT EXISTS other *late* supplier      <=> min = max over late lines only
-- (l1 is itself late, so that min/max is never NULL for qualifying rows).
WITH order_suppliers AS (
    SELECT
        l_orderkey AS os_orderkey,
        min(l_suppkey) AS min_supp,
        max(l_suppkey) AS max_supp,
        min(CASE WHEN l_receiptdate > l_commitdate THEN l_suppkey END) AS min_late_supp,
        max(CASE WHEN l_receiptdate > l_commitdate THEN l_suppkey END) AS max_late_supp
    FROM
        lineitem
    GROUP BY
        l_orderkey
)
SELECT
    s_name,
    count(*) AS numwait
FROM
    lineitem l1
    JOIN orders ON o_orderkey = l1.l_orderkey
    JOIN supplier ON s_suppkey = l1.l_suppkey
    JOIN nation ON n_nationkey = s_nationkey
    JOIN order_suppliers ON os_orderkey = l1.l_orderkey
WHERE
    o_orderstatus = 'F'
    AND l1.l_receiptdate > l1.l_commitdate
    AND n_name = 'SAUDI ARABIA'
    AND min_supp <> max_supp
    AND min_late_supp = max_late_supp
GROUP BY
    s_name
ORDER BY
    numwait DESC,
    s_name
LIMIT 100;
