select
    c_name,
    c_custkey,
    orders.o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(l_quantity) as total_qty -- Added alias here
from
    customer,
    orders,
    lineitem
where
    orders.o_orderkey in (
        select
            o_orderkey
        from
            lineitem
        group by
            o_orderkey
        having
            sum(l_quantity) > 312
    )
  and c_custkey = o_custkey
  and orders.o_orderkey = lineitem.o_orderkey
group by
    c_name,
    c_custkey,
    orders.o_orderkey,
    o_orderdate,
    o_totalprice
order by
    o_totalprice desc,
    o_orderdate
limit 100
