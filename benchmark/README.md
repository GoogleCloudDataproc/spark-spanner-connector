# Spark Spanner Connector Benchmark

This benchmark is designed to test the performance of the Spark Spanner Connector, particularly for write operations. It can be run on Google Cloud Dataproc or Databricks.

## Getting Started

This guide walks through setting up your Google Cloud environment to run the Spark Spanner Connector benchmarks.

### 1. Configure Your Environment

The benchmark runner and supporting sbt task read a JSON file in the `benchmark` directory to configure your GCP project, Spanner instance, Dataproc cluster, and benchmark parameters. 
Each task accepts one parameter -- path to the JSON file. If it's missing,  `benchmark.json` is used.

Create a `benchmark.json` file with the following structure. You will fill in the values in the next steps.

```json
{
  "projectId": "your-gcp-project-id",
  "instanceId": "your-spanner-instance-id",
  "databaseId": "your-spanner-database-id",
  "writeTable": "your-spanner-table-name",
  "spannerRegion": "your-gcp-region",
  "dataprocCluster": "spark-spanner-benchmark-cluster",
  "dataprocRegion": "your-gcp-region",
  "dataprocBucket": "your-dataproc-staging-bucket",
  "resultsBucket": "your-benchmark-results-bucket",
  "numRecords": 100000,
  "mutationsPerTransaction": 1000
}
```

### 2. Set up Your GCP Project

Make sure you have the Google Cloud SDK (`gcloud`) installed and authenticated.

```bash
# Set your project ID
gcloud config set project your-gcp-project-id

# Set your region (matching spannerRegion and dataprocRegion)
gcloud config set compute/region your-gcp-region
```

You'll also need to enable the Spanner and Dataproc APIs:
```bash
gcloud services enable spanner.googleapis.com
gcloud services enable dataproc.googleapis.com
```

### 3. Create Spanner Resources

Use the provided sbt tasks to create the Spanner instance, database, and table. These tasks read their configuration from `benchmark.json`.

```bash
# Create a Spanner instance (ensure spannerRegion is set in benchmark.json or use --region argument)
# Example: sbt "createSpannerInstance --region us-central1"
sbt createSpannerInstance

# Create a Spanner database
sbt createSpannerDatabase

# Create the table for the write benchmark
sbt createSpannerTable
```
*Note: The `createSpannerTable` task will read the DDL from `./ddl/create_test_table.sql` and replace "TransferTest" with the `writeTable` value from your `benchmark.json`.*

### 4. Create GCS Buckets

The benchmark requires two GCS buckets:
*   A staging bucket for Dataproc.
*   A bucket to store benchmark results.

You can create these using `gsutil`:
```bash
gsutil mb -p your-gcp-project-id -l your-gcp-region gs://your-dataproc-staging-bucket/
gsutil mb -p your-gcp-project-id -l your-gcp-region gs://your-benchmark-results-bucket/
```
Alternatively, the `createResultsBucket` sbt task can create the results bucket for you.

```bash
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
- Databricks CLI (if using Databricks)

## Authentication

The benchmark authenticates to Google Cloud Spanner using the service account of the Dataproc cluster's VM instances.

When the Dataproc cluster is created using the `createDataprocCluster` task, it is configured with the `https://www.googleapis.com/auth/cloud-platform` scope. This scope grants the cluster's service account broad access to Google Cloud APIs, including Spanner.

This means that as long as the service account has the necessary IAM permissions for Spanner (e.g., `roles/spanner.databaseUser`), the benchmark will be able to authenticate and write to the Spanner table.

There is no need to configure any additional authentication credentials (like service account keys) in the benchmark code or options.

## Workflow

The benchmark is designed to be run against a locally built version of the Spark Spanner Connector. This allows you to test changes you've made to the connector before creating a pull request.

The general workflow is:
1.  Build and install the connector from your feature branch.
2.  Build the benchmark, which packages the locally installed connector.
3.  Run the benchmark on your Spark cluster.

### Step 1: Build and Install the Connector

1.  Check out the branch of the connector that you want to test (e.g., your feature branch with write support).
2.  Build and install the connector to your local Maven repository. This makes it available to the benchmark project.

    ```bash
    # From the root of the spark-spanner-connector repository
    mvn clean install -P3.3
    ```

### Step 2: Build the Benchmark

The benchmark is configured to be packaged as a self-contained "fat JAR" that includes the connector and all its dependencies.

1.  Navigate to the `benchmark` directory.
2.  Build the fat JAR using `sbt-assembly`.

    ```bash
    # From the benchmark directory
    sbt assembly
    ```
    This will create a JAR file in the `target/scala-2.12/` directory, for example: `spanner-spark-benchmark-assembly-0.1.jar`.

### Step 3: Run the Benchmark

You can run the benchmark on Google Cloud Dataproc. The `build.sbt` file provides convenient tasks for this.

Before running, you need to configure your environment in `benchmark.json`. This file contains all the settings for your GCP project, Spanner instance, Dataproc cluster, and benchmark parameters.

#### Creating a Dataproc Cluster

The `createDataprocCluster` task can be used to create a new Dataproc cluster for running the benchmark.

**Configuration:**

This task reads the following properties from `benchmark.json`:
- `dataprocCluster`: The name for the new cluster.
- `dataprocRegion`: The region for the cluster.
- `dataprocBucket`: The GCS bucket to be associated with the cluster.
- `projectId`: Your Google Cloud project ID.

**Command:**

The task accepts the following optional arguments to override the values in `benchmark.json`:
- `--numWorkers`: The number of worker nodes.
- `--masterMachineType`: The machine type for the master node.
- `--workerMachineType`: The machine type for the worker nodes.
- `--imageVersion`: The Dataproc image version.

```bash
# Example from the benchmark directory
sbt "createDataprocCluster --numWorkers 4"
```

#### Creating the Results Bucket

The `createResultsBucket` task creates a GCS bucket to store the JSON results from benchmark runs.

**Configuration:**

This task reads the following properties from `benchmark.json`:
- `resultsBucket`: The name of the GCS bucket to create.
- `projectId`: Your Google Cloud project ID.
- `dataprocRegion`: The location for the bucket (e.g., `us-central1`).

**Command:**

```bash
# From the benchmark directory
sbt createResultsBucket
```
This command will create the bucket if it does not already exist.

#### Running on Google Cloud Dataproc

The `runDataproc` task submits the benchmark job to a Dataproc cluster.

**Configuration:**

This task reads all the necessary configuration from `benchmark.json`, including:
- `dataprocCluster`, `dataprocRegion`, `dataprocBucket`, `projectId`
- `resultsBucket`
- `instanceId`, `databaseId`, `writeTable`
- `numRecords`, `mutationsPerTransaction`

**Command:**

The `runDataproc` task can accept arguments to override the values in `benchmark.json`. The arguments are passed to the Spark job.
- `numRecords`
- `writeTable`
- `databaseId`
- `instanceId`
- `mutationsPerTransaction` (optional)

```bash
# Example from the benchmark directory, using settings from benchmark.json
sbt runDataproc

# Example overriding some parameters
sbt "runDataproc 1000000 my_test_table"
```

## Benchmark Results

After a benchmark run is complete, the results are stored as a JSON file in the results GCS bucket.

### Location

You can find the results in the bucket specified by the `resultsBucket` property in your `benchmark.json` file.

The directory structure and file naming convention is as follows:
- **Bucket:** `gs://<results_bucket_name>/`
- **Directory:** `/<benchmark_name>/`
- **File:** `/<run_id>.json`

For example:
`gs://my-spark-spanner-bench-results/SparkSpannerWriteBenchmark/2026-01-07T12-00-00Z_a1b2c3d4.json`

Each JSON file contains detailed information about the run, including performance metrics, configuration parameters, and versions. For the detailed schema, see `RESULTS_SCHEMA.md`.