-- TPC-H tables for ClickHouse (columnar), MergeTree engine.
-- Types mirror postgres_tables.sql: integer->Int32, bigint->Int64,
-- numeric(15,2)->Decimal(15,2), date->Date, varchar->String.
-- LowCardinality(String) on columns with <~10k distinct values (dictionary
-- encoding - standard ClickHouse tuning, roughly what a columnar warehouse
-- does automatically).
--
-- ORDER BY is the sparse primary index + on-disk sort order; it is NOT unique.
-- Fact tables are partitioned by year: date-range pruning for reports, and
-- partition-level REPLACE/DROP is the main tool for applying late corrections.

CREATE TABLE region (
    r_regionkey  Int32,
    r_name       LowCardinality(String),
    r_comment    String
) ENGINE = MergeTree ORDER BY r_regionkey;

CREATE TABLE nation (
    n_nationkey  Int32,
    n_name       LowCardinality(String),
    n_regionkey  Int32,
    n_comment    String
) ENGINE = MergeTree ORDER BY n_nationkey;

CREATE TABLE supplier (
    s_suppkey    Int32,
    s_name       String,
    s_address    String,
    s_nationkey  Int32,
    s_phone      String,
    s_acctbal    Decimal(15,2),
    s_comment    String
) ENGINE = MergeTree ORDER BY s_suppkey;

CREATE TABLE customer (
    c_custkey     Int32,
    c_name        String,
    c_address     String,
    c_nationkey   Int32,
    c_phone       String,
    c_acctbal     Decimal(15,2),
    c_mktsegment  LowCardinality(String),
    c_comment     String
) ENGINE = MergeTree ORDER BY c_custkey;

CREATE TABLE part (
    p_partkey      Int32,
    p_name         String,
    p_mfgr         LowCardinality(String),
    p_brand        LowCardinality(String),
    p_type         LowCardinality(String),
    p_size         Int32,
    p_container    LowCardinality(String),
    p_retailprice  Decimal(15,2),
    p_comment      String
) ENGINE = MergeTree ORDER BY p_partkey;

CREATE TABLE partsupp (
    ps_partkey     Int32,
    ps_suppkey     Int32,
    ps_availqty    Int32,
    ps_supplycost  Decimal(15,2),
    ps_comment     String
) ENGINE = MergeTree ORDER BY (ps_partkey, ps_suppkey);

CREATE TABLE orders (
    o_orderkey       Int64,
    o_custkey        Int32,
    o_orderstatus    LowCardinality(String),
    o_totalprice     Decimal(15,2),
    o_orderdate      Date,
    o_orderpriority  LowCardinality(String),
    o_clerk          LowCardinality(String),
    o_shippriority   Int32,
    o_comment        String
) ENGINE = MergeTree
PARTITION BY toYear(o_orderdate)
ORDER BY (o_orderdate, o_orderkey);

CREATE TABLE lineitem (
    l_orderkey       Int64,
    l_partkey        Int32,
    l_suppkey        Int32,
    l_linenumber     Int32,
    l_quantity       Decimal(15,2),
    l_extendedprice  Decimal(15,2),
    l_discount       Decimal(15,2),
    l_tax            Decimal(15,2),
    l_returnflag     LowCardinality(String),
    l_linestatus     LowCardinality(String),
    l_shipdate       Date,
    l_commitdate     Date,
    l_receiptdate    Date,
    l_shipinstruct   LowCardinality(String),
    l_shipmode       LowCardinality(String),
    l_comment        String
) ENGINE = MergeTree
PARTITION BY toYear(l_shipdate)
ORDER BY (l_shipdate, l_orderkey, l_linenumber);
