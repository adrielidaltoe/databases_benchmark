-- Built after COPY. Each statement is timed individually by load_postgres.py.
-- No foreign keys: they add load cost and the planner doesn't use them for
-- TPC-H join selection. Referential integrity is guaranteed by dbgen.

-- Primary keys
ALTER TABLE region   ADD PRIMARY KEY (r_regionkey);
ALTER TABLE nation   ADD PRIMARY KEY (n_nationkey);
ALTER TABLE supplier ADD PRIMARY KEY (s_suppkey);
ALTER TABLE customer ADD PRIMARY KEY (c_custkey);
ALTER TABLE part     ADD PRIMARY KEY (p_partkey);
ALTER TABLE partsupp ADD PRIMARY KEY (ps_partkey, ps_suppkey);
ALTER TABLE orders   ADD PRIMARY KEY (o_orderkey);
ALTER TABLE lineitem ADD PRIMARY KEY (l_orderkey, l_linenumber);

-- Join columns (foreign-key side)
CREATE INDEX nation_n_regionkey_idx   ON nation   (n_regionkey);
CREATE INDEX supplier_s_nationkey_idx ON supplier (s_nationkey);
CREATE INDEX customer_c_nationkey_idx ON customer (c_nationkey);
CREATE INDEX partsupp_ps_suppkey_idx  ON partsupp (ps_suppkey);
CREATE INDEX orders_o_custkey_idx     ON orders   (o_custkey);
CREATE INDEX lineitem_part_supp_idx   ON lineitem (l_partkey, l_suppkey);   -- Q9, Q17, Q20
CREATE INDEX lineitem_l_suppkey_idx   ON lineitem (l_suppkey);              -- Q5, Q7, Q15, Q21

-- Range-filter columns (date predicates in most TPC-H queries)
CREATE INDEX orders_o_orderdate_idx   ON orders   (o_orderdate);
CREATE INDEX lineitem_l_shipdate_idx  ON lineitem (l_shipdate);
