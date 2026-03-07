#!/usr/bin/env python

# Copyright 2023 Google LLC. All Rights Reserved.
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

import os
from pyspark.sql import SparkSession

def main():
    project_id = os.environ["SPANNER_PROJECT_ID"]
    instance_id = os.environ["SPANNER_INSTANCE_ID"]
    database_id = os.environ["SPANNER_DATABASE_ID"]

    # Configure a Spark session with a Spanner catalog named "spanner".
    # This allows reading and writing tables with SQL and DataFrame APIs
    # without specifying connection options on every operation.
    spark = (SparkSession.builder
        .appName("SparkSpannerDemo")
        .config("spark.sql.catalog.spanner",
                "com.google.cloud.spark.spanner.SpannerCatalog")
        .config("spark.sql.catalog.spanner.projectId", project_id)
        .config("spark.sql.catalog.spanner.instanceId", instance_id)
        .config("spark.sql.catalog.spanner.databaseId", database_id)
        .getOrCreate())

    # --- Read via the catalog ---
    # Tables are referenced as <catalog>.<table>.
    df = spark.sql("SELECT * FROM spanner.people")
    df.printSchema()
    df.show()

    # --- Read via the DataSource API (format) ---
    # This approach still works and does not require catalog configuration.
    df2 = spark.read.format('cloud-spanner') \
        .option("projectId", project_id) \
        .option("instanceId", instance_id) \
        .option("databaseId", database_id) \
        .option("table", "people") \
        .load()
    df2.show()

    # --- Write via the DataSource API ---
    columns = ['id', 'name', 'email']
    data = [(1, 'John Doe', 'john@example.com'),
            (2, 'Jane Doe', 'jane@example.com')]
    write_df = spark.createDataFrame(data, columns)

    write_df.write.format('cloud-spanner') \
        .option("projectId", project_id) \
        .option("instanceId", instance_id) \
        .option("databaseId", database_id) \
        .option("table", "people") \
        .mode("append") \
        .save()

    # --- Write via the catalog (SQL) ---
    # INSERT INTO uses the catalog, so no connection options are needed.
    spark.sql("""
        INSERT INTO spanner.people
        VALUES (3, 'Bob Smith', 'bob@example.com')
    """)

    # --- Write via the catalog (DataFrame) ---
    # writeTo references the catalog table directly.
    write_df2 = spark.createDataFrame(
        [(4, 'Alice Wong', 'alice@example.com')], columns)
    write_df2.writeTo("spanner.people").append()

if __name__ == '__main__':
    main()
