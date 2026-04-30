SELECT
    lineitem.o_orderkey AS l_orderkey,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate AS o_orderdate,
    o_shippriority AS o_shippriority
FROM
    customer,
    orders,
    lineitem
WHERE
    c_mktsegment = 'HOUSEHOLD'
  AND c_custkey = o_custkey
  AND lineitem.o_orderkey = orders.o_orderkey
  AND o_orderdate < DATE '1995-03-18'
  AND l_shipdate > DATE '1995-03-18'
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC,
    o_orderdate,
    l_orderkey,
    o_shippriority
    LIMIT 10
