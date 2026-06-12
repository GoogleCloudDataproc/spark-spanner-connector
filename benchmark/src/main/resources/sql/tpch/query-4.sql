select
    o_orderpriority,
    count(*) as order_count
from
    orders
where
    o_orderdate >= date '1995-09-01'
  and o_orderdate < DATE '1995-12-01'
  and EXISTS
    (
        select
            *
        from
            lineitem
        where
            lineitem.o_orderkey = orders.o_orderkey
          and l_commitdate < l_receiptdate
    )
group by
    o_orderpriority
order by
    o_orderpriority