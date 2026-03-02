#!/bin/bash
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

# This script automates the setup of GoogleSQL and PostgreSQL test databases
# for Spark-Spanner connector integration tests. It performs the following actions:
#
# 1. Environment Variable Check: Verifies the presence of essential variables:
#    - SPANNER_PROJECT_ID: Your Google Cloud project ID.
#    - SPANNER_INSTANCE_ID: The ID for the Cloud Spanner instance.
#    - SPANNER_DATABASE_ID: The base name for the test databases.
#
# 2. Command-Line Tool Check: Ensures that the 'gcloud' CLI is installed and
#    accessible in the system's PATH.
#
# 3. Instance Management:
#    - Checks if the specified Cloud Spanner instance exists.
#    - If the instance is not found, it creates a new one with a default
#      configuration (1 node in regional-us-central1).
#
# 4. Database Management:
#    - Uses SPANNER_DATABASE_ID directly for the GoogleSQL database.
#    - Creates a PostgreSQL database with a "-pg" suffix based on SPANNER_DATABASE_ID.
#    - For both databases:
#      - If the database exists, it drops and recreates it to ensure a clean state.
#      - If the database does not exist, it creates it.
#
# 5. Schema and Data Population:
#    - Applies the initial DDL schema to create tables from .sql files.
#    - Executes DML statements to populate the tables with test data from .sql files.
#

# Function to display an error message and exit
fail() {
  echo "$1" >&2
  exit 1
}

# 1. Check for required environment variables
[[ -z "$SPANNER_PROJECT_ID" ]] && fail "Error: SPANNER_PROJECT_ID is not set."
[[ -z "$SPANNER_INSTANCE_ID" ]] && fail "Error: SPANNER_INSTANCE_ID is not set."
[[ -z "$SPANNER_DATABASE_ID" ]] && fail "Error: SPANNER_DATABASE_ID is not set."

# 2. Check for gcloud CLI
command -v gcloud >/dev/null 2>&1 || fail "Error: gcloud command not found. Please install the Google Cloud SDK."

# 3. Define database IDs (no randomization, ignore SPANNER_USE_EXISTING_DATABASE)
DATABASE_ID="$SPANNER_DATABASE_ID"
DATABASE_ID_PG="${DATABASE_ID}-pg"

# 4. Check and create Spanner instance if it doesn't exist
echo "Checking for Spanner instance '$SPANNER_INSTANCE_ID'..."
if ! gcloud spanner instances describe "$SPANNER_INSTANCE_ID" --project="$SPANNER_PROJECT_ID" &>/dev/null; then
  echo "Instance not found. Creating a new instance..."
  gcloud spanner instances create "$SPANNER_INSTANCE_ID" \
    --project="$SPANNER_PROJECT_ID" \
    --config="regional-us-central1" \
    --description="Test Instance for Spark-Spanner Connector" \
    --nodes=1
  echo "Instance '$SPANNER_INSTANCE_ID' created."
else
  echo "Instance '$SPANNER_INSTANCE_ID' already exists."
fi

# 5. Define DDL and DML file paths
DB_RESOURCES_DIR="spark-3.1-spanner-lib/src/test/resources/db"
DDL_GOOGLESOL_FILES=(
  "$DB_RESOURCES_DIR/populate_ddl.sql"
  "$DB_RESOURCES_DIR/populate_ddl_graph.sql"
)
DML_GOOGLESOL_FILES=(
  "$DB_RESOURCES_DIR/insert_data.sql"
  "$DB_RESOURCES_DIR/insert_data_graph.sql"
)
DDL_PG_FILES=("$DB_RESOURCES_DIR/populate_ddl_pg.sql")
DML_PG_FILES=("$DB_RESOURCES_DIR/insert_data_pg.sql")

# Function to execute a multi-statement DML file
execute_dml_file() {
  local db_id=$1
  local file=$2
  echo "Executing DML file: $file"

  local content
  content=$(<"$file")
  # Remove comments and newlines to allow splitting by semicolon.
  local statements
  statements=$(echo "$content" | grep -v '^--' | tr '\n' ' ' | sed 's/;/;\n/g')

  while IFS= read -r stmt; do
    # Check for non-empty statements
    if [[ -n "$(echo "$stmt" | tr -d '[:space:]')" ]]; then
      echo "Executing DML: ${stmt:0:80}..."
      
      local max_retries=5
      local attempt=1
      local delay=1
      while true; do
        if gcloud spanner databases execute-sql "$db_id" --instance="$SPANNER_INSTANCE_ID" --project="$SPANNER_PROJECT_ID" --sql="$stmt"; then
          # Success
          break
        else
          # Failure
          if [[ $attempt -eq $max_retries ]]; then
            echo "DML statement failed after $max_retries attempts. Aborting."
            exit 1
          fi
          echo "DML statement failed. Retrying in ${delay}s... (Attempt ${attempt}/${max_retries})"
          sleep $delay
          # Exponential backoff
          delay=$((delay * 2))
          attempt=$((attempt + 1))
        fi
      done
    fi
  done <<< "$statements"
}

# Function to manage and populate a database
setup_database() {
  local db_id=$1
  local dialect=$2
  local ddl_files_str=$3
  local dml_files_str=$4

  echo "Setting up $dialect database '$db_id'..."

  if gcloud spanner databases describe "$db_id" --instance="$SPANNER_INSTANCE_ID" --project="$SPANNER_PROJECT_ID" &>/dev/null; then
    echo "Database '$db_id' already exists. Dropping and recreating to ensure clean state..."
    gcloud spanner databases delete "$db_id" --instance="$SPANNER_INSTANCE_ID" --project="$SPANNER_PROJECT_ID" --quiet
    gcloud spanner databases create "$db_id" --instance="$SPANNER_INSTANCE_ID" --project="$SPANNER_PROJECT_ID" --database-dialect="$dialect"
  else
    echo "Database '$db_id' not found. Creating..."
    gcloud spanner databases create "$db_id" --instance="$SPANNER_INSTANCE_ID" --project="$SPANNER_PROJECT_ID" --database-dialect="$dialect"
  fi

  # Apply DDL
  local ddl_files_arr
  read -r -a ddl_files_arr <<< "$ddl_files_str"
  for ddl in "${ddl_files_arr[@]}"; do
    echo "Applying DDL from: $ddl"
    gcloud spanner databases ddl update "$db_id" --instance="$SPANNER_INSTANCE_ID" --project="$SPANNER_PROJECT_ID" --ddl-file="$ddl"
  done

  # Apply DML
  for dml_file in $dml_files_str; do
    execute_dml_file "$db_id" "$dml_file"
  done

  echo "$dialect database '$db_id' setup complete."
}

# Setup GoogleSQL database
setup_database "$DATABASE_ID" "GOOGLE_STANDARD_SQL" "${DDL_GOOGLESOL_FILES[*]}" "${DML_GOOGLESOL_FILES[*]}"

# Setup PostgreSQL database
setup_database "$DATABASE_ID_PG" "POSTGRESQL" "${DDL_PG_FILES[*]}" "${DML_PG_FILES[*]}"

echo "All test databases have been successfully configured."
