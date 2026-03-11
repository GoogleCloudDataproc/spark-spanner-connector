# Spark Spanner Connector — Live Demo

This folder contains demo scripts for showcasing write support and Spark Catalog
features on **Dataproc** and **Databricks**.

All demos create and clean up their own tables — no pre-requisite DDL needed.

## Demo Artifacts

| File | Platform | What it shows |
|------|----------|---------------|
| `examples/demo/dataproc_write_demo.py` | Dataproc | DataFrame API writes (append, mutation types, overwrite) + Catalog SQL (CREATE/INSERT/SELECT/DROP) |
| `examples/demo/databricks_dataframe_write.py` | Databricks | DataFrame API writes — append, mutation types, overwrite, partial row updates |
| `examples/demo/databricks_catalog_sql.py` | Databricks | Catalog SQL — CREATE TABLE, INSERT INTO, SELECT, Ignore mode, ErrorIfExists, DROP TABLE |

---

## Building the Connector JAR

From the repository root, build the Spark 3.1 shaded JAR:

```bash
./mvnw package clean install -P3.1 -DskipTests
```

The output JAR will be at:

```
spark-3.1-spanner/target/spark-3.1-spanner-0.0.1-SNAPSHOT.jar
```

Set a convenience variable for the commands below:

```bash
export CONNECTOR_JAR="spark-3.1-spanner/target/spark-3.1-spanner-0.0.1-SNAPSHOT.jar"
```

---

## Dataproc

### Environment Variables

```bash
export SPANNER_PROJECT_ID="<PROJECT_ID>"
export SPANNER_INSTANCE_ID="<SPANNER_INSTANCE_ID>"
export SPANNER_DATABASE_ID="<SPANNER_DATABASE_ID>"
export SPANNER_DATAPROC_CLUSTER="<DATAPROC_CLUSTER_NAME>"
export SPANNER_DATAPROC_REGION="<GCP_REGION>"
export SPANNER_DATAPROC_BUCKET="<GCS_BUCKET>"
```

### Upload JAR to GCS

```bash
gsutil cp "$CONNECTOR_JAR" "gs://${SPANNER_DATAPROC_BUCKET}/spark-spanner-connector.jar"
```

### Submit the Job

```bash
gcloud dataproc jobs submit pyspark \
    --cluster "$SPANNER_DATAPROC_CLUSTER" \
    --region "$SPANNER_DATAPROC_REGION" \
    --jars "gs://${SPANNER_DATAPROC_BUCKET}/spark-spanner-connector.jar" \
    --properties "spark.dynamicAllocation.enabled=false,spark.spanner.projectId=$SPANNER_PROJECT_ID,spark.spanner.instanceId=$SPANNER_INSTANCE_ID,spark.spanner.databaseId=$SPANNER_DATABASE_ID" \
    examples/demo/dataproc_write_demo.py
```

The Spanner connection details are passed as Spark properties (`spark.spanner.*`)
and read by the script at runtime. The Spanner catalog is configured inside the
script itself.

---

## Databricks

### Upload JAR to Databricks

```bash
export DATABRICKS_HOST="<DATABRICKS_HOST>"
```

Upload the locally-built JAR using the Databricks CLI:

```bash
# Upload to a Unity Catalog Volume
databricks fs cp "$CONNECTOR_JAR" \
    "dbfs:/Volumes/spark_spanner_connector_ws/default/connector-jars/mksyunz/spark-3.1-spanner-0.0.1-SNAPSHOT.jar"
```

Or upload via the Databricks UI: **Catalog** → select your Volume →
**Upload to this volume** → select the JAR file.

### Cluster Configuration

Your Databricks cluster needs:

1. **Connector library**: Libraries tab → Install new → Library Source / Volumes →
   `/Volumes/<CATALOG>/default/spanner_connector_assets/spark-3.1-spanner-0.0.1-SNAPSHOT.jar`

2. **Environment variables** (Advanced Options → Spark → Environment Variables):
   ```
   GCP_PROJECT_ID=<PROJECT_ID>
   SPANNER_INSTANCE_ID=<SPANNER_INSTANCE_ID>
   GCP_CREDENTIALS={{secrets/<scope>/<key>}}
   ```

3. **Init script** for GCP credentials (see [`examples/databricks/setup_gcp_credentials.sh`](../databricks/setup_gcp_credentials.sh)).

Restart the cluster after making changes.

### Import & Run

1. Import `examples/demo/databricks_dataframe_write.py` and `examples/demo/databricks_catalog_sql.py` into
   your Databricks workspace.
2. Attach each notebook to the configured cluster.
3. Run all cells top-to-bottom.

### Demo Flow

**Notebook 1 — DataFrame API** (`examples/demo/databricks_dataframe_write.py`):
- Creates a table via the catalog for the demo
- Writes rows with `mode("append")`
- Demonstrates `mutationType` options (`insert`, `update`)
- Shows `mode("overwrite")` with `overwriteMode=truncate`
- Partial row update with `enablePartialRowUpdates=true`
- Cleans up

**Notebook 2 — Catalog SQL** (`examples/demo/databricks_catalog_sql.py`):
- `CREATE TABLE` with `TBLPROPERTIES('primaryKeys' = '...')`
- `INSERT INTO` and `SELECT` via pure SQL cells
- `CREATE TABLE IF NOT EXISTS` (Ignore mode) — second call is a no-op
- `writeTo().create()` (ErrorIfExists) — second call raises an error
- `DROP TABLE` cleanup

---

## Notes

- Both Databricks notebooks set the catalog config programmatically via
  `spark.conf.set(...)`. If your cluster already has these in its Spark config,
  you can remove the setup cells.
- The `database_id` is hardcoded to `repo-test` in the Databricks notebooks.
- All demo table names start with `demo_` for easy identification.
