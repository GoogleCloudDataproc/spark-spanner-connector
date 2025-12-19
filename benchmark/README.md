# Spark Spanner Connector Benchmark

This benchmark is designed to test the performance of the Spark Spanner Connector, particularly for write operations. It can be run on Google Cloud Dataproc or Databricks.

## Prerequisites

Before you begin, make sure you have the following tools installed:
- Java (version 8 or higher)
- Apache Maven
- sbt (Scala Build Tool)
- Google Cloud SDK (`gcloud`)
- Databricks CLI (if using Databricks)

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

You can run the benchmark on either Google Cloud Dataproc or Databricks. The `build.sbt` file provides convenient tasks for this.

#### Running on Google Cloud Dataproc

The `runDataproc` task submits the benchmark job to a Dataproc cluster.

**Configuration:**

Before running, you must set the following environment variables:
- `SPANNER_DATAPROC_CLUSTER`: The name of your Dataproc cluster.
- `SPANNER_DATAPROC_REGION`: The region of your cluster.
- `SPANNER_DATAPROC_BUCKET`: The GCS bucket name for staging the JARs (not the full gs:// URI).
- `SPANNER_PROJECT_ID`: Your Google Cloud project ID.

**Command:**

The benchmark accepts the following arguments, which you pass to the `runDataproc` task:
- `numRecords`: The number of records to write.
- `writeTable`: The name of the Spanner table to write to.
- `mutationsPerTransaction` (optional): The number of mutations per transaction (default: 5000).

```bash
# Example from the benchmark directory
sbt "runDataproc 1000000 my_test_table"
```

#### Running on Databricks

NOTE: This is work in progress.

The `runDatabricks` task submits the benchmark job to a Databricks cluster.

**Configuration:**

- You must set the `SPANNER_DATABRICKS_CLUSTER_ID` environment variable to the ID of your Databricks cluster.

**Command:**

The benchmark arguments are the same as for Dataproc.

```bash
# Example from the benchmark directory
export SPANNER_DATABRICKS_CLUSTER_ID="<your-cluster-id>"
sbt "runDatabricks 1000000 my_test_table"
```