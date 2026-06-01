#!/bin/bash

# Ensure required Spanner environment variables are set
: "${SPANNER_INSTANCE_ID:?Error: SPANNER_INSTANCE_ID environment variable is not set. Use: export SPANNER_INSTANCE_ID=<your spanner instance id>}"
: "${SPANNER_DATABASE_ID:?Error: SPANNER_DATABASE_ID environment variable is not set. Use: export SPANNER_DATABASE_ID=<your spanner database id>}"

QUERY_DIR="./src/main/resources/sql/tpch"

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
        gcloud spanner databases execute-sql "$SPANNER_DATABASE_ID" \
            --instance="$SPANNER_INSTANCE_ID" \
            --sql="$(cat "$FILE")" \
            --query-mode=PROFILE
    else
        echo "Skipping $FILE: File not found."
    fi
done
