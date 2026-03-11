# Databricks notebook source
# MAGIC %md
# MAGIC # Spark Spanner Connector — Catalog & SQL Demo
# MAGIC
# MAGIC This notebook demonstrates managing Spanner tables with **Spark SQL** through the Spanner Catalog:
# MAGIC
# MAGIC 1. **CREATE TABLE** with primary keys
# MAGIC 2. **INSERT INTO** rows
# MAGIC 3. **SELECT** / query data
# MAGIC 4. **CREATE TABLE IF NOT EXISTS** (Ignore save mode)
# MAGIC 5. **ErrorIfExists** via `writeTo().create()`
# MAGIC 6. **DROP TABLE**
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - Cluster has the Spark Spanner Connector library installed (JAR or Maven).
# MAGIC - Cluster environment variables: `GCP_PROJECT_ID`, `SPANNER_INSTANCE_ID`.
# MAGIC - GCP credentials configured (e.g. via init script).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — Configure the Spanner Catalog

# COMMAND ----------

import os

project_id  = os.environ["GCP_PROJECT_ID"]
instance_id = os.environ["SPANNER_INSTANCE_ID"]
database_id = "repo-test"

spark.conf.set("spark.sql.catalog.spanner",
               "com.google.cloud.spark.spanner.SpannerCatalog")
spark.conf.set("spark.sql.catalog.spanner.projectId",  project_id)
spark.conf.set("spark.sql.catalog.spanner.instanceId", instance_id)
spark.conf.set("spark.sql.catalog.spanner.databaseId", database_id)

print(f"✓ Catalog configured — {project_id} / {instance_id} / {database_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. CREATE TABLE
# MAGIC
# MAGIC Create a Spanner table directly from Spark SQL.
# MAGIC Primary keys are specified via `TBLPROPERTIES`.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS spanner.demo_cities

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE spanner.demo_cities (
# MAGIC     id     BIGINT NOT NULL,
# MAGIC     city   STRING,
# MAGIC     state  STRING,
# MAGIC     temp_f DOUBLE
# MAGIC ) USING `cloud-spanner`
# MAGIC TBLPROPERTIES('primaryKeys' = 'id')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. INSERT INTO
# MAGIC
# MAGIC Insert rows using standard SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO spanner.demo_cities VALUES
# MAGIC   (1, 'San Francisco', 'CA', 62.0),
# MAGIC   (2, 'New York',      'NY', 45.0),
# MAGIC   (3, 'Austin',        'TX', 85.0),
# MAGIC   (4, 'Seattle',       'WA', 55.0),
# MAGIC   (5, 'Denver',        'CO', 70.0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. SELECT — Query Spanner from SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM spanner.demo_cities ORDER BY id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT city, temp_f
# MAGIC FROM spanner.demo_cities
# MAGIC WHERE temp_f > 60
# MAGIC ORDER BY temp_f DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. CREATE TABLE IF NOT EXISTS (Ignore Mode)
# MAGIC
# MAGIC If the table already exists, the statement is a **no-op** — existing data is untouched.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS spanner.demo_ignore

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS spanner.demo_ignore (
# MAGIC     id   BIGINT NOT NULL,
# MAGIC     note STRING
# MAGIC ) USING `cloud-spanner`
# MAGIC TBLPROPERTIES('primaryKeys' = 'id')

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO spanner.demo_ignore VALUES (1, 'original data')

# COMMAND ----------

# MAGIC %md
# MAGIC Running `CREATE TABLE IF NOT EXISTS` again — the table already exists so nothing happens:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS spanner.demo_ignore (
# MAGIC     id   BIGINT NOT NULL,
# MAGIC     note STRING
# MAGIC ) USING `cloud-spanner`
# MAGIC TBLPROPERTIES('primaryKeys' = 'id')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Data is still intact
# MAGIC SELECT * FROM spanner.demo_ignore

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. ErrorIfExists — `writeTo().create()`
# MAGIC
# MAGIC The DataFrame `writeTo().create()` API creates a new table and writes data in one step.
# MAGIC If the table already exists, it raises an error.

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS spanner.demo_error_if_exists")

df = spark.createDataFrame(
    [(1, "hello"), (2, "world")],
    ["id", "message"],
)

# First call: creates the table and writes data
df.writeTo("spanner.demo_error_if_exists") \
  .tableProperty("primaryKeys", "id") \
  .create()

print("✓ Table created and data written.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM spanner.demo_error_if_exists ORDER BY id

# COMMAND ----------

# Second call: should fail because the table already exists
try:
    df.writeTo("spanner.demo_error_if_exists") \
      .tableProperty("primaryKeys", "id") \
      .create()
    print("✗ Expected an error but did not get one!")
except Exception as e:
    print(f"✓ Correctly raised error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. DROP TABLE
# MAGIC
# MAGIC Clean up all demo tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS spanner.demo_cities;
# MAGIC DROP TABLE IF EXISTS spanner.demo_ignore;
# MAGIC DROP TABLE IF EXISTS spanner.demo_error_if_exists

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✓ Demo Complete
# MAGIC
# MAGIC We demonstrated:
# MAGIC - **CREATE TABLE** with primary keys via `TBLPROPERTIES`
# MAGIC - **INSERT INTO** and **SELECT** via standard SQL
# MAGIC - **CREATE TABLE IF NOT EXISTS** (Ignore save mode)
# MAGIC - **writeTo().create()** (ErrorIfExists save mode)
# MAGIC - **DROP TABLE** cleanup
