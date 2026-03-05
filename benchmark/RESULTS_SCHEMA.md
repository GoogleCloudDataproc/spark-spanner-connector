# Benchmark Results JSON Schema and File Convention

This document defines the schema for the JSON files that store the results of benchmark runs, as well as the directory structure and naming convention for these files in the GCS bucket.

## JSON Schema

```json
{
  "runId": "string - A unique identifier for the benchmark run",
  "runTimestamp": "string - ISO 8601 timestamp of the benchmark run",
  "benchmarkName": "string - Name of the benchmark (e.g., SparkSpannerWriteBenchmark)",
  "clusterSparkVersion": "string - The version of Spark used",
  "connectorVersion": "string - The version of the Spanner-Spark connector",
  "spannerConfig": {
    "projectId": "string",
    "instanceId": "string",
    "databaseId": "string",
    "table": "string"
  },
  "benchmarkParameters": {
    "numRecords": "long",
    "mutationsPerTransaction": "int",
    "bytesPerTransaction": "long",
    "numWriteThreads": "int",
    "maxPendingTransactions": "int",
    "assumeIdempotentRows": "boolean"
  },
  "performanceMetrics": {
    "durationSeconds": "double - Total duration of the write operation in seconds",
    "throughputMbPerSec": "double - Throughput in MB/s",
    "totalSizeMb": "double - Total size of the generated data in MB",
    "recordCount": "long - Number of records written"
  },
  "sparkConfig": {
    "numPartitions": "int"
  }
}
```

## Directory Structure and File Naming Convention

The results will be stored in the GCS bucket specified in the configuration.

*   **Bucket:** `gs://<results_bucket_name>/`
*   **Directory Structure:** `/<benchmark_name>/`
*   **File Name:** `/<run_id>.json`

### Example

`gs://my-spark-spanner-bench-results/SparkSpannerWriteBenchmark/2026-01-07T12-00-00Z_a1b2c3d4.json`

The filename is a combination of a timestamp and a short, 8-character unique ID to ensure it's unique and sortable. The `runId` field in the JSON schema corresponds to this 8-character unique ID.

