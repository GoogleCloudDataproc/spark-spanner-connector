#!/bin/sh

cp ../customer.tbl customer.csv
cp ../lineitem.tbl lineitem.csv
cp ../nation.tbl nation.csv
cp ../orders.tbl orders.csv
cp ../part.tbl part.csv
cp ../partsupp.tbl partsupp.csv
cp ../region.tbl region.csv
cp ../supplier.tbl supplier.csv

perl -i -pe 's/\|$//' customer.csv
perl -i -pe 's/\|$//' lineitem.csv
perl -i -pe 's/\|$//' orders.csv
perl -i -pe 's/\|$//' part.csv
perl -i -pe 's/\|$//' partsupp.csv
perl -i -pe 's/\|$//' region.csv
perl -i -pe 's/\|$//' supplier.csv
