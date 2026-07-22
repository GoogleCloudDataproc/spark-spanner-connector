#!/usr/bin/env python
# Copyright 2026 Google Inc. All Rights Reserved.
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

import sys
import json
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col
from pyspark.sql.types import *
from decimal import Decimal
from datetime import date, datetime


def load_table(spark, project_id, instance_id, database_id, table):
    return (
        spark.read.format("cloud-spanner")
        .option("projectId", project_id)
        .option("instanceId", instance_id)
        .option("databaseId", database_id)
        .option("table", table)
        .option("enablePredicateSql", True)
        .load()
    )

def run_inner_join_tests(orders, lineitem, issues):
    print("\nrun_inner_join_tests")

    joined = (
        orders.alias("o")
        .join(
            lineitem.alias("l"),
            col("o.O_ORDERKEY") == col("l.O_ORDERKEY"),
            "inner"
        )
    )

    actual = joined.count()
    expected = 13

    if actual != expected:
        issues.append(
            f"Inner join expected {expected} rows but found {actual}"
        )

def run_join_projection_tests(orders, lineitem, issues):
    print("\nrun_join_projection_tests")

    joined = (
        orders.alias("o")
        .join(
            lineitem.alias("l"),
            col("o.O_ORDERKEY") == col("l.O_ORDERKEY"),
            "inner"
        )
        .select(
            col("o.O_ORDERKEY"),
            col("o.O_CUSTKEY"),
            col("l.L_PARTKEY"),
            col("l.L_QUANTITY")
        )
    )

    expected_columns = [
        "O_ORDERKEY",
        "O_CUSTKEY",
        "L_PARTKEY",
        "L_QUANTITY",
    ]

    if joined.columns != expected_columns:
        issues.append(
            f"Join projection expected columns {expected_columns} but found {joined.columns}"
        )

    expected_rows = 13
    actual_rows = joined.count()

    if actual_rows != expected_rows:
        issues.append(
            f"Join projection expected {actual_rows} rows but found {actual_rows}"
        )

def run_join_filter_tests(orders, lineitem, issues):
    print("\nrun_join_filter_tests")

    joined = (
        orders.alias("o")
        .join(
            lineitem.alias("l"),
            col("o.O_ORDERKEY") == col("l.O_ORDERKEY"),
            "inner"
        )
        .filter(col("l.L_QUANTITY") > 20)
        .collect()
    )

    expected = 10

    if len(joined) != expected:
        issues.append(...)

def run_join_value_tests(orders, lineitem, issues):
    print("\nrun_join_value_tests")

    row = (
        orders.alias("o")
        .join(
            lineitem.alias("l"),
            col("o.O_ORDERKEY") == col("l.O_ORDERKEY"),
            "inner"
        )
        .filter(col("o.O_ORDERKEY") == 1)
        .select(
            col("o.O_CUSTKEY"),
            col("l.L_PARTKEY"),
            col("l.L_LINENUMBER")
        )
        .first()
    )

    if row.O_CUSTKEY != 36901:
        issues.append(...)

    if row.L_PARTKEY != 155190:
        issues.append(...)

def write_results(spark, output_path, issues):
    status = "PASS" if not issues else "FAIL: " + " | ".join(issues)

    print(status)

    (
        spark.createDataFrame([Row(summary=status)])
        .coalesce(1)
        .write.mode("overwrite")
        .csv(output_path)
    )

def main():
    print("\n\nRead Acceptance Test - join pushdown\n\n")

    spark = SparkSession.builder.appName('Read Acceptance Test on Spark - join pushdown').getOrCreate()

    print("spark.version: ", spark.version)

    output_path = sys.argv[1]
    project_id = sys.argv[2]
    instance_id = sys.argv[3]
    database_id = sys.argv[4]

    orders = load_table(
        spark,
        project_id,
        instance_id,
        database_id,
        "ORDERS",
    )

    lineitem = load_table(
        spark,
        project_id,
        instance_id,
        database_id,
        "LINEITEM",
    )

    print('The resulting schema are')
    print('ORDERS')
    orders.printSchema()
    print('LINEITEM')
    lineitem.printSchema()

    issues = []
    run_inner_join_tests(orders, lineitem, issues)
    run_join_projection_tests(orders, lineitem, issues)
    run_join_filter_tests(orders, lineitem, issues)
    write_results(spark, output_path, issues)

if __name__ == '__main__':
  main()
