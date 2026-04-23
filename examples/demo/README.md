# Spark Spanner Connector — Demo

This folder contains a demo notebook for showcasing write support and Spark
Catalog features on **Dataproc** with Jupyter as well as a notebook showing how to map from Spark
`STRUCT` to Spanner `JSON` and back.

All demos create and clean up their own tables — no pre-requisite DDL needed.

## Demo Artifacts

| File                                   | What it shows                                                                                      |
|----------------------------------------|----------------------------------------------------------------------------------------------------|
| `examples/demo/dataproc_write_demo.ipynb` | DataFrame API writes (append, mutation types, overwrite) + Catalog SQL (CREATE/INSERT/SELECT/DROP) |
| `examples/demo/dataproc_json_demo.ipynb`  | Working with Spark `STRUCT` and Spanner `JSON`                                                     |

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

### Create a Dataproc Cluster with Jupyter Support

```bash
gcloud dataproc clusters create "$SPANNER_DATAPROC_CLUSTER" \
    --region "$SPANNER_DATAPROC_REGION" \
    --optional-components=JUPYTER \
    --enable-component-gateway \
    --bucket "$SPANNER_DATAPROC_BUCKET" \
    --properties "spark:spark.jars=gs://${SPANNER_DATAPROC_BUCKET}/spark-spanner-connector.jar,spark:spark.dynamicAllocation.enabled=false"
```

Once the cluster is running, open the **JupyterLab** link from the Dataproc
cluster's **Web Interfaces** tab in the Cloud Console.

> **Note:** The cluster's compute service account must have permissions to
> connect to Spanner (e.g. the `roles/spanner.databaseUser` role). By default,
> Dataproc VMs use the Compute Engine default service account — make sure it
> has the necessary Spanner IAM bindings on your instance/database.

### Upload the SparkSQL Magic Wheel

The demo notebooks use the
[`sparksql_magic`](https://github.com/cryeo/sparksql-magic) IPython extension to
render SQL results as formatted tables. Download the wheel from PyPI and upload
it to the same GCS bucket so the notebooks can install it at runtime:

```bash
pip download sparksql-magic --no-deps -d .
gsutil cp sparksql_magic*.whl "gs://${SPANNER_DATAPROC_BUCKET}/spark-sql/"
```

Each notebook's first code cell copies the wheel from GCS and runs
`%pip install` automatically — no manual installation is needed.

### Run the Notebook

1. Upload `examples/demo/dataproc_write_demo.ipynb` and `examples/demo/dataproc_json_demo.ipynb` to JupyterLab (or create a
   new notebook and paste the contents).
2. Run all cells top-to-bottom.

The Spanner connection details are passed as Spark properties (`spark.spanner.*`)
set during cluster creation and read by the notebook at runtime. The Spanner
catalog is configured inside the notebook itself.

---

## Notes

- All demo table names start with `demo_` for easy identification.
