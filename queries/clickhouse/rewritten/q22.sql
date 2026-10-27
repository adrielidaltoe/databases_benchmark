-- TPC-H Q22, decorrelated: NOT EXISTS (orders for this customer) becomes a
-- LEFT JOIN to a CTE of customers with orders + IS NULL (anti-join); the
-- uncorrelated avg(c_acctbal) becomes a one-row CTE, read as a scalar so it is
-- computed once. (A CROSS JOIN to it made Postgres misestimate the anti-join at
-- 1 row and re-aggregate the CTE once per candidate: 0.3s -> 114s at SF1.)
WITH candidates AS (
    SELECT
        c_custkey,
        substring(c_phone FROM 1 FOR 2) AS cntrycode,
        c_acctbal
    FROM
        customer
    WHERE
        substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
),
avg_balance AS (
    SELECT
        avg(c_acctbal) AS avg_acctbal
    FROM
        candidates
    WHERE
        c_acctbal > 0.00
),
customers_with_orders AS (
    SELECT DISTINCT
        o_custkey
    FROM
        orders
)
SELECT
    cntrycode,
    count(*) AS numcust,
    sum(c_acctbal) AS totacctbal
FROM
    candidates
    LEFT JOIN customers_with_orders ON o_custkey = c_custkey
WHERE
    c_acctbal > (SELECT avg_acctbal FROM avg_balance)
    AND o_custkey IS NULL
GROUP BY
    cntrycode
ORDER BY
    cntrycode
-- ClickHouse default join_use_nulls=0 fills unmatched LEFT JOIN columns with 0, not NULL,
-- so o_custkey IS NULL would never match. Force SQL semantics (same as q13).
SETTINGS join_use_nulls = 1;
