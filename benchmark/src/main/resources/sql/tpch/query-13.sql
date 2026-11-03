select count(orders.O_ORDERKEY)
from
    orders as orders inner join lineitem lineitem on
        orders.O_ORDERKEY = lineitem.O_ORDERKEY AND
        orders.C_CUSTKEY = lineitem.C_CUSTKEY
