#!/bin/sh

perl -pe 's/\|$//' customer.tbl > customer.csv
perl -pe 's/\|$//' orders.tbl > orders.csv
perl -pe 's/\|$//' part.tbl > part.csv
perl -pe 's/\|$//' partsupp.tbl > partsupp.csv
perl -pe 's/\|$//' region.tbl > region.csv
perl -pe 's/\|$//' supplier.tbl > supplier.csv
perl -pe 's/\|$//' nation.tbl > nation.csv

# Add C_CUSTKEY to LINEITEM using ORDERS.O_ORDERKEY -> ORDERS.O_CUSTKEY
perl -F'\|' -lane '
    BEGIN {
        open(my $fh, "<", "orders.csv") or die "Cannot open orders.csv: $!";
        while (<$fh>) {
            chomp;
            my @fields = split(/\|/, $_, -1);
            $order_to_customer{$fields[0]} = $fields[1];
        }
        close($fh);
    }

    my $order_key = $F[0];
    die "No customer key found for O_ORDERKEY=$order_key\n"
        unless exists $order_to_customer{$order_key};

    print $order_to_customer{$order_key}, "|", join("|", @F);
' lineitem.tbl | perl -pe 's/\|$//' > lineitem.csv
