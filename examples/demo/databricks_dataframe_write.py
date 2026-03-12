# Databricks notebook source
# MAGIC %md
# MAGIC # Spark Spanner Connector — DataFrame API Write Demo
# MAGIC
# MAGIC This notebook demonstrates writing to Google Cloud Spanner using the DataFrame API:
# MAGIC
# MAGIC 1. **Append** mode — insert new rows
# MAGIC 2. **Mutation types** — `insert`, `update`, `insert_or_update`, `replace`
# MAGIC 3. **Overwrite** mode — `truncate` and `recreate`
# MAGIC 4. **Partial row updates** — write a subset of columns
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - Cluster has the Spark Spanner Connector library installed (JAR or Maven).
# MAGIC - GCP credentials configured (e.g. via init script).

# COMMAND ----------

SPANNER_OPTS = {
    "projectId":  "improvingvancouver",
    "instanceId": "mksyunz-spark-dev",
    "databaseId": "repo-test",
}

print(f"Spanner: {SPANNER_OPTS['projectId']}/{SPANNER_OPTS['instanceId']}/{SPANNER_OPTS['databaseId']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — Spanner DDL Helper
# MAGIC
# MAGIC Databricks Unity Catalog owns catalog resolution, so we cannot register a custom
# MAGIC `TableCatalog` for Spanner. Instead, we call the Spanner Admin API directly
# MAGIC via `spark._jvm` (the connector JAR is already on the classpath).

# COMMAND ----------

def spanner_ddl(ddl_statements):
    """Execute DDL statements against Spanner using the connector's Java classes."""
    jvm = spark._jvm
    java_map = jvm.java.util.HashMap()
    for k, v in SPANNER_OPTS.items():
        java_map.put(k, v)
    ci_map = jvm.org.apache.spark.sql.util.CaseInsensitiveStringMap(java_map)
    spanner = jvm.com.google.cloud.spark.spanner.SpannerUtils \
        .buildSpannerOptions(ci_map).getService()
    try:
        admin = spanner.getDatabaseAdminClient()
        java_list = jvm.java.util.ArrayList()
        for stmt in ddl_statements:
            java_list.add(stmt)
        admin.updateDatabaseDdl(
            SPANNER_OPTS["instanceId"], SPANNER_OPTS["databaseId"], java_list, None
        ).get()
    finally:
        spanner.close()

print("✓ Spanner DDL helper ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — Create demo table
# MAGIC
# MAGIC We create the target table from scratch using the Spanner Admin API, then demonstrate writes with the DataFrame API.

# COMMAND ----------

try:
    spanner_ddl(["DROP TABLE demo_df_write"])
except Exception:
    pass  # table may not exist yet

spanner_ddl(["""
    CREATE TABLE demo_df_write (
        id     INT64 NOT NULL,
        name   STRING(MAX),
        dept   STRING(MAX),
        salary FLOAT64
    ) PRIMARY KEY (id)
"""])
print("✓ Created table: demo_df_write")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Append Mode
# MAGIC
# MAGIC The default — inserts rows into the existing table.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType

schema = StructType([
    StructField("id",     LongType(),   False),
    StructField("name",   StringType(), True),
    StructField("dept",   StringType(), True),
    StructField("salary", DoubleType(), True),
])

data = [
    (1, "Alice",   "Engineering", 120000.0),
    (2, "Bob",     "Marketing",    95000.0),
    (3, "Charlie", "Engineering", 130000.0),
]
df = spark.createDataFrame(data, schema)

(df.write.format("cloud-spanner")
    .options(**SPANNER_OPTS)
    .option("table", "demo_df_write")
    .mode("append")
    .save())

print("✓ Wrote 3 rows with append mode.")

# COMMAND ----------

# Read back and display
(spark.read.format("cloud-spanner")
    .options(**SPANNER_OPTS)
    .option("table", "demo_df_write")
    .load()
    .orderBy("id")
    .show())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Mutation Types
# MAGIC
# MAGIC The `mutationType` option controls the Spanner mutation used:
# MAGIC - `insert` — fails if the row already exists
# MAGIC - `update` — fails if the row does not exist
# MAGIC - `insert_or_update` (default) — upserts
# MAGIC - `replace` — replaces the entire row

# COMMAND ----------

# INSERT a new row
new_row = spark.createDataFrame([(4, "Diana", "Sales", 105000.0)], schema)

(new_row.write.format("cloud-spanner")
    .options(**SPANNER_OPTS)
    .option("table", "demo_df_write")
    .option("mutationType", "insert")
    .mode("append")
    .save())

print("✓ Inserted row id=4 with mutationType='insert'.")

# COMMAND ----------

# UPDATE an existing row (change Bob's salary)
update_row = spark.createDataFrame([(2, "Bob", "Marketing", 100000.0)], schema)

(update_row.write.format("cloud-spanner")
    .options(**SPANNER_OPTS)
    .option("table", "demo_df_write")
    .option("mutationType", "update")
    .mode("append")
    .save())

print("✓ Updated row id=2 with mutationType='update'. Bob's salary is now 100k.")

# COMMAND ----------

(spark.read.format("cloud-spanner")
    .options(**SPANNER_OPTS)
    .option("table", "demo_df_write")
    .load()
    .orderBy("id")
    .show())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Overwrite Mode (Truncate)
# MAGIC
# MAGIC `mode("overwrite")` with `overwriteMode=truncate` deletes all existing rows, then writes the new data. The table schema is preserved.

# COMMAND ----------

replacement = spark.createDataFrame([
    (10, "New-Alice", "Design",  140000.0),
    (20, "New-Bob",   "Finance", 110000.0),
], schema)

(replacement.write.format("cloud-spanner")
    .options(**SPANNER_OPTS)
    .option("table", "demo_df_write")
    .option("overwriteMode", "truncate")
    .mode("overwrite")
    .save())

print("✓ Overwrote table with 2 new rows (truncate mode).")

# COMMAND ----------

(spark.read.format("cloud-spanner")
    .options(**SPANNER_OPTS)
    .option("table", "demo_df_write")
    .load()
    .orderBy("id")
    .show())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Partial Row Updates
# MAGIC
# MAGIC With `enablePartialRowUpdates=true`, you can write only a subset of columns.
# MAGIC This is useful for updating specific fields without touching the rest.

# COMMAND ----------

# Update only the salary column for id=10
partial_schema = StructType([
    StructField("id",     LongType(),   False),
    StructField("salary", DoubleType(), True),
])
partial_df = spark.createDataFrame([(10, 150000.0)], partial_schema)

(partial_df.write.format("cloud-spanner")
    .options(**SPANNER_OPTS)
    .option("table", "demo_df_write")
    .option("mutationType", "update")
    .option("enablePartialRowUpdates", "true")
    .mode("append")
    .save())

print("✓ Partial update: set salary=150k for id=10 (other columns unchanged).")

# COMMAND ----------

(spark.read.format("cloud-spanner")
    .options(**SPANNER_OPTS)
    .option("table", "demo_df_write")
    .load()
    .orderBy("id")
    .show())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

spanner_ddl(["DROP TABLE demo_df_write"])
print("✓ Dropped demo_df_write.")
