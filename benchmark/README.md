# Spark Spanner Connector Benchmark

This benchmark is designed to test the performance of the Spark Spanner Connector, particularly for write operations. It can be run on Google Cloud Dataproc.

## Getting Started

This guide walks through setting up your Google Cloud environment to run the Spark Spanner Connector benchmarks.

### 1. Configure Your Environment

The benchmark runner and supporting sbt tasks now use a structured configuration:
*   **Benchmark Definitions**: Defined in `benchmark/benchmark_definitions.json`. These describe a known tests -- combination of a Spark job, job parameters and Spark environment.
*   **Data Sources**: Defined in `benchmark/data_sources.json`. These map logical data names to DDLs, allowing separation of schema from specific table names.
*   **Environment Configuration**: Defined in `benchmark/environment.json`. This file contains all environment-specific settings (GCP project IDs, Dataproc cluster names, GCS bucket names, Spanner instance IDs, Databricks host/token/cluster IDs, etc.) and mappings from logical data source names to physical table names in your environment.

You need to create and configure your local `environment.json` file:

1.  Copy the template:
    ```bash
    cp benchmark/environment.json.template benchmark/environment.json
    ```
2.  Open `benchmark/environment.json` in your editor and fill in the values for the `dataproc` or `databricks` sections with your specific details.

**Important for Databricks GCS Access**: If running benchmarks on Databricks and writing results to GCS, ensure that the `application_default_credentials.json` file is provisioned to each worker node's filesystem at `/root/.config/gcloud/application_default_credentials.json`. The benchmark notebook expects this file to be present at that exact location.

**Important**: `benchmark/environment.json` is in `.gitignore` and should **not** be committed to the repository.

### 2. Set up Your GCP Project

Make sure you have the Google Cloud SDK (`gcloud`) installed and authenticated.

```bash
# Set your project ID
gcloud config set project your-gcp-project-id

# Set your region (matching spannerRegion)
gcloud config set compute/region your-gcp-region
```

You'll also need to enable the Spanner and Dataproc APIs:
```bash
gcloud services enable spanner.googleapis.com
gcloud services enable dataproc.googleapis.com
```

### 3. Create Spanner Resources

Use the provided sbt tasks to ensure the necessary Spanner instance, database, and table exist for your benchmark run. These tasks read their configuration (project ID, instance ID, database ID, etc.) from the `environment.json` file.

```bash
# Ensure Spanner instance and database exist for a specific benchmark scenario.
# This task will create the instance and database if they don't exist.
# Example: sbt "spannerUp dataproc-100mil-records"
sbt "spannerUp <benchmark_name>"

# Create the Spanner table required for a specific benchmark scenario.
# This task uses the DDL defined in `data_sources.json` for the given benchmark.
# Example: sbt "createBenchmarkSpannerTable dataproc-100mil-records"
sbt "createBenchmarkSpannerTable <benchmark_name>"
```
*Note: The `createBenchmarkSpannerTable` task will infer the DDL file from `data_sources.json` based on the benchmark's `dataSource` and automatically replace the placeholder table name with the `writeTable` value derived from your benchmark configuration.*

### 4. Create GCS Buckets

The benchmark requires two GCS buckets:
*   A staging bucket for Dataproc.
*   A bucket to store benchmark results.

You can create the results bucket using the `createResultsBucket` sbt task. You'll need to create the Dataproc staging bucket manually.

```bash
# Create the Dataproc staging bucket
gsutil mb -p your-gcp-project-id -l your-gcp-region gs://your-dataproc-staging-bucket/

# Create the GCS bucket for benchmark results
sbt createResultsBucket
```

### 5. Service Account and Permissions

The benchmarks run on Dataproc and authenticate to Spanner using the VM's service account. This service account needs permissions to access Spanner and GCS.

By default, Dataproc clusters use the project's default Compute Engine service account. For simplicity, you can grant this service account the required roles.

```bash
# Get your project number
PROJECT_NUMBER=$(gcloud projects describe your-gcp-project-id --format="value(projectNumber)")

# Get your default Compute Engine service account email
SERVICE_ACCOUNT_EMAIL="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

# Grant roles
# These roles are generally sufficient for the benchmark to run.
gcloud projects add-iam-policy-binding your-gcp-project-id --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" --role="roles/spanner.databaseUser"
gcloud projects add-iam-policy-binding your-gcp-project-id --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" --role="roles/spanner.databaseAdmin"
gcloud projects add-iam-policy-binding your-gcp-project-id --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" --role="roles/storage.objectAdmin"
```

## Prerequisites

Before you begin, make sure you have the following tools installed:
- Java (version 8 or higher)
- Apache Maven
- sbt (Scala Build Tool)
- Google Cloud SDK (`gcloud`)
- `jq` (a lightweight and flexible command-line JSON processor)

## Authentication

The benchmark handles authentication differently depending on the environment:

### Google Cloud Dataproc

On Google Cloud Dataproc, the benchmark authenticates to Google Cloud Spanner using the service account of the cluster's VM instances.
When the Dataproc cluster is created, it should be configured with appropriate scopes (e.g., `https://www.googleapis.com/auth/cloud-platform`).
As long as the cluster's service account has the necessary IAM permissions for Spanner (e.g., `roles/spanner.databaseUser`) and GCS (e.g., `roles/storage.objectAdmin`), no additional authentication configuration is needed within the benchmark code.

### Databricks

Authentication for both Google Cloud Spanner and Google Cloud Storage is handled via a single service account, provisioned to the cluster using an init script. This provides a unified and secure way to grant GCP access to your Databricks jobs.

#### 1. Create a Databricks Secret

First, you need to store your GCP service account's JSON key file in a Databricks secret.

1.  Create a secret scope. For example, `gcp-credentials`.
    ```bash
    databricks secrets create-scope gcp-credentials
    ```
2.  Store your service account key in the scope. Let's call the secret `spanner-benchmark-sa`.
    ```bash
    databricks secrets put-secret gcp-credentials spanner-benchmark-sa --json-file /path/to/your/service-account.json
    ```

#### 2. Configure the Cluster Init Script

The `benchmark/setup_gcp_credentials.sh` script is designed to run as a cluster-scoped init script. It reads the secret you just created and installs it as the Application Default Credentials (ADC) file on each node in the cluster.

1.  **Upload the init script**: Upload `benchmark/setup_gcp_credentials.sh` to a location on your Databricks workspace or DBFS (e.g., `dbfs:/databricks/init_scripts/setup_gcp_credentials.sh`).
2.  **Configure the cluster**: In your Databricks cluster configuration, navigate to "Advanced Options" -> "Init Scripts". Add the path to the script you just uploaded.
3.  **Set Environment Variables**: In the same cluster configuration, under "Advanced Options" -> "Spark", set the following environment variable. This tells the init script which secret to read.
    ```
    GCP_CREDENTIALS={{secrets/gcp-credentials/spanner-benchmark-sa}}
    ```
    Replace `gcp-credentials` and `spanner-benchmark-sa` with the scope and secret name you created in step 1.

When the cluster starts, the init script will run on every node, creating the file `/root/.config/gcloud/application_default_credentials.json`. The Spark Spanner connector and GCS connector will automatically pick up and use these credentials.

#### 3. Required GCP IAM Roles

The service account you use must have permissions to access both Spanner and the GCS bucket for results. Grant the following roles to your service account on your GCP project:

*   **For Spanner Access**:
    *   `roles/spanner.databaseUser`: Allows reading from and writing data to Spanner databases.
    *   `roles/spanner.databaseAdmin`: Required by the setup tasks (`spannerUp`, `createBenchmarkSpannerTable`) to create and manage database schemas.
*   **For GCS Access**:
    *   `roles/storage.objectAdmin`: Allows writing benchmark results to your GCS bucket.

## Benchmarking Workflow

This section describes how to run benchmarks using sbt tasks.

### sbt Tasks Overview

*   `sbt "runBenchmark <benchmark_name>"`: Builds the connector, runs the specified benchmark on your Dataproc or Databricks cluster, and outputs the GCS path of the result file.
*   `sbt "setBenchmarkBaseline <benchmark_name> <gcs_path>"`: Copies a specific benchmark run's result (identified by its full GCS path) to establish it as the baseline for future comparisons.
*   `sbt "compareBenchmarkResults <benchmark_name> <gcs_path>"`: Downloads a specific benchmark run's result (identified by its full GCS path) and its corresponding baseline, then outputs a formatted comparison report.

### Workflow

1.  **Build the Connector**: From the root of the repository, build the connector and install it into your local Maven repository. This makes it available to the benchmark project.
    ```bash
    # Use the Spark version that matches your benchmark environment
    mvn clean install -P3.3 -DskipTests
    ```
2.  **Run a Benchmark and Establish Baseline**:
    *   Navigate to the `benchmark` directory.
    *   Run your chosen benchmark (e.g., `dataproc-100mil-records`):
        ```bash
        cd benchmark
        sbt "runBenchmark dataproc-100mil-records"
        ```
    *   The script will print the GCS path where the result JSON was uploaded (e.g., `gs://<your-results-bucket>/SparkSpannerWriteBenchmark/<timestamp>_<githash>.json`). Note down the full GCS path.
    *   Use this path to set it as the baseline:
        ```bash
        sbt "setBenchmarkBaseline dataproc-100mil-records <full_gcs_path_from_above>"
        ```
    This step effectively tags a known-good performance run as your reference point.

3.  **Make Code Changes and Compare**:
    *   Make any desired code changes to the Spark-Spanner connector.
    *   Re-build the connector to ensure your changes are included:
        ```bash
        # From the project root
        mvn clean install -P3.3 -DskipTests
        ```
    *   Run the same benchmark again to generate new results:
        ```bash
        # Still in the benchmark directory
        sbt "runBenchmark dataproc-100mil-records"
        ```
    *   Note down the GCS path for the new run from the output.
    *   Compare the new results against your established baseline:
        ```bash
        sbt "compareBenchmarkResults dataproc-100mil-records <full_gcs_path_for_new_run>"
        ```
    The task will output a formatted comparison report, showing performance deltas between your baseline and the new run.

## Benchmark Results

After a benchmark run is complete, the results are stored as a JSON file in the results GCS bucket.

### Location

You can find the results in the bucket specified by the `resultsBucket` property in your `environment.json` file.

The directory structure and file naming convention is as follows:
- **Bucket:** `gs://<results_bucket_name>/`
- **Directory:** `/SparkSpannerWriteBenchmark/`
- **File:** `/<run_id>.json`

For example:
`gs://my-spark-spanner-bench-results/SparkSpannerWriteBenchmark/2026-01-07T12-00-00Z_a1b2c3d4.json`

Each JSON file contains detailed information about the run, including performance metrics, configuration parameters, and versions. For the detailed schema, see `RESULTS_SCHEMA.md`.

## Troubleshooting

### "Error: Catalog 'X' is not accessible in current workspace"

This error indicates a mismatch between the Databricks workspace targeted by your CLI configuration and the one specified in `environment.json`, or a lack of proper permissions.

**Possible Causes and Solutions:**

1.  **Workspace Host Mismatch**:
    *   **Diagnosis**: Your `databricks auth describe` command might show a different `Host` than the `databricksHost` value in your `environment.json`. The `sbt` task uses the host from `environment.json`.
    *   **Solution**: Update the `databricksHost` in `environment.json` to match the host of the Databricks workspace where your Unity Catalog is correctly configured and accessible. Ensure all other Databricks-specific settings (`clusterId`, `ucVolumePath`) are valid for this workspace.

2.  **Insufficient Permissions**: Even if the catalog is bound to the workspace, the user or service principal associated with your `databricksToken` might lack the necessary permissions.
    *   **Diagnosis**:
        *   Identify your user: `databricks auth describe`
        *   List accessible catalogs: `databricks catalogs list` (check if your catalog is listed)
        *   Check catalog permissions: `databricks grants get catalog your_catalog_name` (look for `USE_CATALOG`)
        *   Check schema permissions: `databricks grants get schema your_catalog_name.your_schema_name` (look for `USE_SCHEMA`)
        *   Check volume permissions: `databricks grants get volume your_catalog_name.your_schema_name.your_volume_name` (look for `WRITE_VOLUME` and `READ_VOLUME`)
    *   **Solution**: A Databricks administrator needs to grant the required privileges to your user or a group you belong to. Specifically, ensure `USE_CATALOG`, `USE_SCHEMA`, and `WRITE_VOLUME`/`READ_VOLUME` (or `ALL_PRIVILEGES`) are granted.