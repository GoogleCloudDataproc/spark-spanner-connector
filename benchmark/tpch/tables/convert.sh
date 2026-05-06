#!/bin/sh

cp ../customer.tbl customer.csv
cp ../lineitem.tbl lineitem.csv
cp ../nation.tbl nation.csv
cp ../orders.tbl orders.csv
cp ../part.tbl part.csv
cp ../partsupp.tbl partsupp.csv
cp ../region.tbl region.csv
cp ../supplier.tbl supplier.csv

#sed -i '' 's/,/|/g; s/|/,/; s/|/,/; s/|/,/; s/|/,/; s/|/,/; s/|/,/; s/|$//' customer.csv
#sed -i '' 's/,/|/g; s/|/,/; s/|/,/; s/|/,/; s/|/,/; s/|/,/; s/|/,/; s/|/,/; s/|/,/; s/|/,/; s/|/,/; s/|/,/; s/|/,/; s/|/,/; s/|/,/; s/|/,/; s/|$//' lineitem.csv.csv
#sed -i '' 's/,/|/g; s/|/,/; s/|/,/; s/|/,/; s/|$//' nation.csv
#sed -i '' 's/,/|/g; s/|/,/; s/|/,/; s/|/,/; s/|/,/; s/|/,/; s/|/,/; s/|/,/; s/|/,/; s/|$//' orders.csv
#sed -i '' 's/,/|/g; s/|/,/; s/|/,/; s/|/,/; s/|/,/; s/|/,/; s/|/,/; s/|/,/; s/|/,/; s/|$//' part.csv
#sed -i '' 's/,/|/g; s/|/,/; s/|/,/; s/|/,/; s/|/,/; s/|$//' partsupp.csv
#sed -i '' 's/,/|/g; s/|/,/; s/|/,/; s/|$//' region.csv
#sed -i '' 's/,/|/g; s/|/,/; s/|/,/; s/|/,/; s/|/,/; s/|/,/; s/|/,/; s/|$//' supplier.csv

sed -i '' 's/|$//' customer.csv
sed -i '' 's/|$//' lineitem.csv
sed -i '' 's/|$//' orders.csv
sed -i '' 's/|$//' part.csv
sed -i '' 's/|$//' partsupp.csv
sed -i '' 's/|$//' region.csv
sed -i '' 's/|$//' supplier.csv
