#!/bin/bash
# This script configures Google Application Default Credentials on a Databricks cluster.
set -e

# This environment variable must be configured on the cluster to point to the Databricks secret.
GCP_CREDENTIALS_CONTENT="$GCP_CREDENTIALS"

if [ -z "$GCP_CREDENTIALS_CONTENT" ]; then
  echo "Error: The GCP_CREDENTIALS environment variable is not set." >&2
  echo "Please configure this in the Spark Cluster Environment Variables:" >&2
  echo "GCP_CREDENTIALS={{secrets/your-secret-scope/your-gcp-key-name}}" >&2
  exit 1
fi

ADC_DIR="/root/.config/gcloud"
ADC_FILE="$ADC_DIR/application_default_credentials.json"

echo "Creating directory for ADC file: $ADC_DIR"
mkdir -p "$ADC_DIR"

echo "Writing credentials to $ADC_FILE"
printf "%s" "$GCP_CREDENTIALS_CONTENT" > "$ADC_FILE"

echo "Successfully configured Google Application Default Credentials."
