#!/bin/bash
#
# This script configures Google Application Default Credentials on a Databricks cluster.
# It reads a GCP service account key from an environment variable and writes it to the
# default location expected by Google Cloud client libraries.

set -e

# The environment variable that holds the content of the GCP credentials JSON.
# This must be configured in the cluster's settings.
GCP_CREDENTIALS_CONTENT="$GCP_CREDENTIALS"

if [ -z "$GCP_CREDENTIALS_CONTENT" ]; then
  echo "Error: The GCP_CREDENTIALS environment variable is not set." >&2
  echo "Please configure this in the Spark Cluster Environment Variables:" >&2
  echo "GCP_CREDENTIALS={{secrets/gcp-credentials/spanner-benchmark-sa}}" >&2
  exit 1
fi

# Path for the Application Default Credentials (ADC) file.
# The script will create this for the user that Spark runs as.
ADC_DIR="/root/.config/gcloud"
ADC_FILE="$ADC_DIR/application_default_credentials.json"

echo "Creating directory for ADC file: $ADC_DIR"
mkdir -p "$ADC_DIR"

echo "Writing credentials to $ADC_FILE"
# Write the content of the environment variable to the file.
cat > "$ADC_FILE" <<EOF
$GCP_CREDENTIALS_CONTENT
EOF

echo "Successfully configured Google Application Default Credentials."
