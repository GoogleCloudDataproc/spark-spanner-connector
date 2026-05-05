#!/bin/bash

# Define your variables
SPANNER_INSTANCE_ID="slord-spark-dev"
SPANNER_DATABASE_ID="test-tpch"
QUERY_DIR="./src/main/resources/sql/tpch"

echo "Running query from $SQL_FILE..."

# Loop from query 1 to 22
for i in {1..22}
do
    Q_NUM=$(printf "%d" $i)
    FILE="$QUERY_DIR/query-$Q_NUM.sql"

    if [ -f "$FILE" ]; then
        echo "---------------------------------------"
        echo "Processing Query: $Q_NUM ($FILE)"
        echo "---------------------------------------"

        # Run the command and capture the output
        # Using --query-mode=PROFILE to get internal Spanner timing
        gcloud spanner databases execute-sql $SPANNER_DATABASE_ID \
            --instance=$SPANNER_INSTANCE_ID \
            --sql="$(cat $FILE)" \
            --query-mode=PROFILE
    else
        echo "Skipping $FILE: File not found."
    fi
done
