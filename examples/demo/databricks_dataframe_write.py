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
# MAGIC - Cluster environment variables: `GCP_PROJECT_ID`, `SPANNER_INSTANCE_ID`.
# MAGIC - GCP credentials configured (e.g. via init script).

# COMMAND ----------

import os

project_id  = os.environ["GCP_PROJECT_ID"]
instance_id = os.environ["SPANNER_INSTANCE_ID"]
database_id = "repo-test"

# Common Spanner connection options
SPANNER_OPTS = {
    "projectId":  project_id,
    "instanceId": instance_id,
    "databaseId": database_id,
}

print(f"Project: {project_id}  Instance: {instance_id}  Database: {database_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — Configure the Spanner Catalog
# MAGIC
# MAGIC Register the Spanner catalog so we can use SQL to create/drop demo tables.

# COMMAND ----------

spark.conf.set("spark.sql.catalog.spanner",
               "com.google.cloud.spark.spanner.SpannerCatalog")
spark.conf.set("spark.sql.catalog.spanner.projectId",  project_id)
spark.conf.set("spark.sql.catalog.spanner.instanceId", instance_id)
spark.conf.set("spark.sql.catalog.spanner.databaseId", database_id)
print("✓ Spanner catalog configured.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — Create demo table
# MAGIC
# MAGIC We create the target table from scratch using the catalog, then demonstrate writes with the DataFrame API.

# COMMAND ----------

TABLE = "demo_df_write"

spark.sql(f"DROP TABLE IF EXISTS spanner.{TABLE}")
spark.sql(f"""
    CREATE TABLE spanner.{TABLE} (
        id     BIGINT NOT NULL,
        name   STRING,
        dept   STRING,
        salary DOUBLE
    ) USING `cloud-spanner`
    TBLPROPERTIES('primaryKeys' = 'id')
""")
print(f"✓ Created table: {TABLE}")

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
    .option("table", TABLE)
    .mode("append")
    .save())

print("✓ Wrote 3 rows with append mode.")

# COMMAND ----------

# Read back and display
(spark.read.format("cloud-spanner")
    .options(**SPANNER_OPTS)
    .option("table", TABLE)
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
    .option("table", TABLE)
    .option("mutationType", "insert")
    .mode("append")
    .save())

print("✓ Inserted row id=4 with mutationType='insert'.")

# COMMAND ----------

# UPDATE an existing row (change Bob's salary)
update_row = spark.createDataFrame([(2, "Bob", "Marketing", 100000.0)], schema)

(update_row.write.format("cloud-spanner")
    .options(**SPANNER_OPTS)
    .option("table", TABLE)
    .option("mutationType", "update")
    .mode("append")
    .save())

print("✓ Updated row id=2 with mutationType='update'. Bob's salary is now 100k.")

# COMMAND ----------

(spark.read.format("cloud-spanner")
    .options(**SPANNER_OPTS)
    .option("table", TABLE)
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
    .option("table", TABLE)
    .option("overwriteMode", "truncate")
    .mode("overwrite")
    .save())

print("✓ Overwrote table with 2 new rows (truncate mode).")

# COMMAND ----------

(spark.read.format("cloud-spanner")
    .options(**SPANNER_OPTS)
    .option("table", TABLE)
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
    .option("table", TABLE)
    .option("mutationType", "update")
    .option("enablePartialRowUpdates", "true")
    .mode("append")
    .save())

print("✓ Partial update: set salary=150k for id=10 (other columns unchanged).")

# COMMAND ----------

(spark.read.format("cloud-spanner")
    .options(**SPANNER_OPTS)
    .option("table", TABLE)
    .load()
    .orderBy("id")
    .show())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS spanner.{TABLE}")
print(f"✓ Dropped {TABLE}.")
