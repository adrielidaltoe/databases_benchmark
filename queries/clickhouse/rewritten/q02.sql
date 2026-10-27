-- TPC-H Q2, decorrelated: the per-part min(ps_supplycost) subquery becomes a
-- CTE computed once for all parts, joined back on (partkey, supplycost).
WITH europe_partsupp AS (
    SELECT
        ps_partkey,
        ps_suppkey,
        ps_supplycost,
        s_acctbal,
        s_name,
        s_address,
        s_phone,
        s_comment,
        n_name
    FROM
        partsupp
        JOIN supplier ON s_suppkey = ps_suppkey
        JOIN nation ON n_nationkey = s_nationkey
        JOIN region ON r_regionkey = n_regionkey
    WHERE
        r_name = 'EUROPE'
),
min_cost AS (
    SELECT
        ps_partkey,
        min(ps_supplycost) AS min_supplycost
    FROM
        europe_partsupp
    GROUP BY
        ps_partkey
)
SELECT
    eps.s_acctbal,
    eps.s_name,
    eps.n_name,
    p_partkey,
    p_mfgr,
    eps.s_address,
    eps.s_phone,
    eps.s_comment
FROM
    part
    JOIN europe_partsupp eps ON eps.ps_partkey = p_partkey
    JOIN min_cost mc ON mc.ps_partkey = p_partkey
        AND mc.min_supplycost = eps.ps_supplycost
WHERE
    p_size = 15
    AND p_type LIKE '%BRASS'
ORDER BY
    eps.s_acctbal DESC,
    eps.n_name,
    eps.s_name,
    p_partkey
LIMIT 100;
