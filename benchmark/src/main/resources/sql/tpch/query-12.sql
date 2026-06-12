SELECT
    l_shipmode,
    SUM(CASE
            WHEN o_orderpriority = '1-URGENT'
                OR o_orderpriority = '2-HIGH'
                THEN 1
            ELSE 0
        END) AS high_line_count,
    SUM(CASE
            WHEN o_orderpriority <> '1-URGENT'
                AND o_orderpriority <> '2-HIGH'
                THEN 1
            ELSE 0
        END) AS low_line_count
FROM
    orders,
    lineitem
WHERE
    orders.o_orderkey = lineitem.o_orderkey
  AND l_shipmode IN ('RAIL', 'AIR')
  AND l_commitdate < l_receiptdate
  AND l_shipdate < l_commitdate
  AND l_receiptdate >= DATE '1997-01-01'
  AND l_receiptdate < DATE '1998-01-01'
GROUP BY
    l_shipmode
ORDER BY
    l_shipmode
