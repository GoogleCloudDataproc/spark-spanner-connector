#!/usr/bin/env python

# Copyright 2026 Google LLC. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Spark Spanner Connector Demo — Write Support & Catalog
=======================================================

This PySpark script demonstrates the connector's write capabilities on Dataproc:

  Part 1 — DataFrame API writes (append, overwrite, mutation types)
  Part 2 — Spark Catalog (CREATE TABLE, INSERT INTO, SELECT, DROP TABLE)

All tables are created and cleaned up by the script itself.

Prerequisites
-------------
  Pass Spanner connection properties via --properties when submitting:
    spark.spanner.projectId
    spark.spanner.instanceId
    spark.spanner.databaseId

Submit to Dataproc
------------------
  gcloud dataproc jobs submit pyspark \\
      --cluster "$SPANNER_DATAPROC_CLUSTER" \\
      --region "$SPANNER_DATAPROC_REGION" \\
      --jars "gs://${SPANNER_DATAPROC_BUCKET}/spark-spanner-connector.jar" \\
      --properties "spark.dynamicAllocation.enabled=false,spark.spanner.projectId=$SPANNER_PROJECT_ID,spark.spanner.instanceId=$SPANNER_INSTANCE_ID,spark.spanner.databaseId=$SPANNER_DATABASE_ID" \\
      examples/demo/dataproc_write_demo.py
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, DoubleType
)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def heading(title):
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60 + "\n")


def run_sql(spark, sql):
    """Print a SQL statement, then execute it."""
    print(f" SQL: {sql}")
    return spark.sql(sql)


def show_code(code):
    """Print a Python statement that is about to be executed."""
    print(f"  PY: {code}")


# ---------------------------------------------------------------------------
# Configuration — read from Spark properties passed via --properties
# ---------------------------------------------------------------------------

def get_config(spark):
    """Read Spanner connection properties from Spark conf."""
    conf = spark.sparkContext.getConf()
    project_id  = conf.get("spark.spanner.projectId",  None)
    instance_id = conf.get("spark.spanner.instanceId", None)
    database_id = conf.get("spark.spanner.databaseId", None)

    if not all([project_id, instance_id, database_id]):
        sys.exit(
            "Error: pass spark.spanner.projectId, spark.spanner.instanceId, "
            "and spark.spanner.databaseId via --properties."
        )
    return project_id, instance_id, database_id


PROJECT_ID = INSTANCE_ID = DATABASE_ID = None  # set in main()

# Table names used in the demo
DF_WRITE_TABLE  = "demo_df_write"
CATALOG_TABLE   = "demo_catalog"
OVERWRITE_TABLE = "demo_overwrite"


def spanner_opts(df_writer, table):
    """Attach standard Spanner connection options to a DataFrameWriter."""
    return (
        df_writer
        .format("cloud-spanner")
        .option("projectId", PROJECT_ID)
        .option("instanceId", INSTANCE_ID)
        .option("databaseId", DATABASE_ID)
        .option("table", table)
    )


def spanner_reader(spark, table):
    """Return a DataFrameReader pointed at a Spanner table."""
    return (
        spark.read.format("cloud-spanner")
        .option("projectId", PROJECT_ID)
        .option("instanceId", INSTANCE_ID)
        .option("databaseId", DATABASE_ID)
        .option("table", table)
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    global PROJECT_ID, INSTANCE_ID, DATABASE_ID

    # ---- Spark session ----
    spark = SparkSession.builder.appName("SpannerWriteDemo").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # ---- Read connection properties from Spark conf ----
    PROJECT_ID, INSTANCE_ID, DATABASE_ID = get_config(spark)

    # ---- Register the Spanner catalog ----
    spark.conf.set("spark.sql.catalog.spanner",
                   "com.google.cloud.spark.spanner.SpannerCatalog")
    spark.conf.set("spark.sql.catalog.spanner.projectId",  PROJECT_ID)
    spark.conf.set("spark.sql.catalog.spanner.instanceId", INSTANCE_ID)
    spark.conf.set("spark.sql.catalog.spanner.databaseId", DATABASE_ID)

    try:
        part1_dataframe_api(spark)
        part2_catalog(spark)
    finally:
        cleanup(spark)
        spark.stop()


# ===================================================================
# Part 1 — DataFrame API Writes
# ===================================================================

def part1_dataframe_api(spark):
    heading("Part 1: DataFrame API Writes")

    # -- 1a. Create the target table via SQL --
    heading("1a: CREATE TABLE")
    run_sql(spark, f"DROP TABLE IF EXISTS spanner.{DF_WRITE_TABLE}")
    run_sql(spark,
        f"CREATE TABLE spanner.{DF_WRITE_TABLE} ("
        f"id BIGINT NOT NULL, name STRING, dept STRING, salary DOUBLE"
        f") USING `cloud-spanner` TBLPROPERTIES('primaryKeys' = 'id')")
    print(f"  ✓ Table {DF_WRITE_TABLE} created.\n")

    # -- 1b. Append mode --
    heading("1b: Write with mode('append')")
    data = [
        (1, "Alice",   "Engineering", 120000.0),
        (2, "Bob",     "Marketing",   95000.0),
        (3, "Charlie", "Engineering", 130000.0),
    ]
    schema = StructType([
        StructField("id",     LongType(),   False),
        StructField("name",   StringType(), True),
        StructField("dept",   StringType(), True),
        StructField("salary", DoubleType(), True),
    ])
    df = spark.createDataFrame(data, schema)

    show_code(f'df.write.format("cloud-spanner").option("table", "{DF_WRITE_TABLE}").mode("append").save()')
    spanner_opts(df.write, DF_WRITE_TABLE).mode("append").save()
    print("  ✓ Wrote 3 rows.")

    show_code(f'spark.read.format("cloud-spanner").option("table", "{DF_WRITE_TABLE}").load().orderBy("id").show()')
    spanner_reader(spark, DF_WRITE_TABLE).load().orderBy("id").show()

    # -- 1c. Mutation type: INSERT --
    heading("1c: Write with mutationType='insert'")
    new_rows = spark.createDataFrame(
        [(4, "Diana", "Sales", 105000.0)], schema
    )
    show_code(f'df.write.format("cloud-spanner").option("table", "{DF_WRITE_TABLE}").option("mutationType", "insert").mode("append").save()')
    (
        spanner_opts(new_rows.write, DF_WRITE_TABLE)
        .option("mutationType", "insert")
        .mode("append")
        .save()
    )
    print("  ✓ Inserted 1 row.")
    spanner_reader(spark, DF_WRITE_TABLE).load().orderBy("id").show()

    # -- 1d. Overwrite mode (truncate) --
    heading("1d: Overwrite with overwriteMode='truncate'")
    run_sql(spark, f"DROP TABLE IF EXISTS spanner.{OVERWRITE_TABLE}")
    run_sql(spark,
        f"CREATE TABLE spanner.{OVERWRITE_TABLE} ("
        f"id BIGINT NOT NULL, name STRING"
        f") USING `cloud-spanner` TBLPROPERTIES('primaryKeys' = 'id')")

    initial = spark.createDataFrame(
        [(1, "old-row-1"), (2, "old-row-2")],
        StructType([
            StructField("id", LongType(), False),
            StructField("name", StringType(), True),
        ]),
    )
    spanner_opts(initial.write, OVERWRITE_TABLE).mode("append").save()
    print("  Seed data:")
    spanner_reader(spark, OVERWRITE_TABLE).load().orderBy("id").show()

    replacement = spark.createDataFrame(
        [(10, "new-row-10"), (20, "new-row-20")],
        StructType([
            StructField("id", LongType(), False),
            StructField("name", StringType(), True),
        ]),
    )
    show_code(f'df.write.format("cloud-spanner").option("table", "{OVERWRITE_TABLE}").option("overwriteMode", "truncate").mode("overwrite").save()')
    (
        spanner_opts(replacement.write, OVERWRITE_TABLE)
        .option("overwriteMode", "truncate")
        .mode("overwrite")
        .save()
    )
    print("  After overwrite (truncate):")
    spanner_reader(spark, OVERWRITE_TABLE).load().orderBy("id").show()


# ===================================================================
# Part 2 — Spark Catalog
# ===================================================================

def part2_catalog(spark):
    heading("Part 2: Spark Catalog (SQL)")

    # -- 2a. CREATE TABLE --
    heading("2a: CREATE TABLE")
    run_sql(spark, f"DROP TABLE IF EXISTS spanner.{CATALOG_TABLE}")
    run_sql(spark,
        f"CREATE TABLE spanner.{CATALOG_TABLE} ("
        f"id BIGINT NOT NULL, city STRING, temp_f DOUBLE"
        f") USING `cloud-spanner` TBLPROPERTIES('primaryKeys' = 'id')")
    print(f"  ✓ Table {CATALOG_TABLE} created.\n")

    # -- 2b. INSERT INTO --
    heading("2b: INSERT INTO")
    run_sql(spark,
        f"INSERT INTO spanner.{CATALOG_TABLE} VALUES "
        f"(1, 'San Francisco', 62.0), (2, 'New York', 45.0), (3, 'Austin', 85.0)")
    print("  ✓ Inserted 3 rows.\n")

    # -- 2c. SELECT --
    heading("2c: SELECT")
    run_sql(spark, f"SELECT * FROM spanner.{CATALOG_TABLE} ORDER BY id").show()

    # -- 2d. writeTo().create() — ErrorIfExists --
    heading("2d: writeTo().create() — ErrorIfExists")
    error_table = CATALOG_TABLE + "_eie"
    run_sql(spark, f"DROP TABLE IF EXISTS spanner.{error_table}")

    df = spark.createDataFrame(
        [(100, "Seattle", 55.0)],
        StructType([
            StructField("id",     LongType(),   False),
            StructField("city",   StringType(), True),
            StructField("temp_f", DoubleType(), True),
        ]),
    )
    show_code(f'df.writeTo("spanner.{error_table}").tableProperty("primaryKeys", "id").create()')
    df.writeTo(f"spanner.{error_table}").tableProperty("primaryKeys", "id").create()
    print(f"  ✓ Created {error_table} and wrote 1 row.")
    run_sql(spark, f"SELECT * FROM spanner.{error_table} ORDER BY id").show()

    # Second create should fail
    show_code(f'df.writeTo("spanner.{error_table}").tableProperty("primaryKeys", "id").create()  # expect error')
    try:
        df.writeTo(f"spanner.{error_table}").tableProperty("primaryKeys", "id").create()
        print("  ✗ Expected error was NOT raised!")
    except Exception as e:
        print(f"  ✓ Second create() correctly failed: {e}\n")

    # -- 2e. CREATE TABLE IF NOT EXISTS (Ignore) --
    heading("2e: CREATE TABLE IF NOT EXISTS — Ignore mode")
    ignore_table = CATALOG_TABLE + "_ign"
    run_sql(spark, f"DROP TABLE IF EXISTS spanner.{ignore_table}")
    run_sql(spark,
        f"CREATE TABLE IF NOT EXISTS spanner.{ignore_table} ("
        f"id BIGINT NOT NULL, note STRING"
        f") USING `cloud-spanner` TBLPROPERTIES('primaryKeys' = 'id')")
    run_sql(spark, f"INSERT INTO spanner.{ignore_table} VALUES (1, 'first')")
    print("  Created table and inserted 1 row.")

    # Second CREATE TABLE IF NOT EXISTS is a no-op
    run_sql(spark,
        f"CREATE TABLE IF NOT EXISTS spanner.{ignore_table} ("
        f"id BIGINT NOT NULL, note STRING"
        f") USING `cloud-spanner` TBLPROPERTIES('primaryKeys' = 'id')")
    print("  Second CREATE TABLE IF NOT EXISTS — no-op.")
    run_sql(spark, f"SELECT * FROM spanner.{ignore_table} ORDER BY id").show()

    # -- 2f. DROP TABLE --
    heading("2f: DROP TABLE")
    run_sql(spark, f"DROP TABLE spanner.{error_table}")
    run_sql(spark, f"DROP TABLE spanner.{ignore_table}")
    print(f"  ✓ Dropped {error_table} and {ignore_table}.\n")


# ===================================================================
# Cleanup
# ===================================================================

def cleanup(spark):
    heading("Cleanup")
    for t in [DF_WRITE_TABLE, CATALOG_TABLE, OVERWRITE_TABLE]:
        try:
            spark.sql(f"DROP TABLE IF EXISTS spanner.{t}")
            print(f"  Dropped {t}")
        except Exception:
            pass
    print("  Done.\n")


if __name__ == "__main__":
    main()
