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


def load_table(spark, project_id, instance_id, database_id, table, **extra_options):
    reader = (
        spark.read.format("cloud-spanner")
        .option("projectId", project_id)
        .option("instanceId", instance_id)
        .option("databaseId", database_id)
        .option("table", table)
        .option("enablePredicateSql", True)
    )

    # Apply any extra options dynamically before loading
    if extra_options:
        reader = reader.options(**extra_options)

    return reader.load()

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

    print("\nrun_inner_join_tests Execution plan:")
    joined.explain(True)

    actual = joined.count()
    expected = 13

    if actual != expected:
        issues.append(
            f"Inner join expected {expected} rows but found {actual}"
        )
        print(f"Inner join expected {expected} rows but found {actual}")

    first = joined.first()

    if first.O_CUSTKEY != 36901:
        issues.append(f"Join value expected 36901 rows but found {first.O_CUSTKEY}")
        print(f"Join value expected 36901 rows but found {first.O_CUSTKEY}")

    if first.L_PARTKEY != 155190:
        issues.append(f"Join value expected 155190 rows but found {first.L_PARTKEY}")
        print(f"Join value expected 155190 rows but found {first.L_PARTKEY}")

    status = "PASS" if not issues else "FAIL: " + " | ".join(issues)

    print(status)


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
            col("l.O_ORDERKEY"),
            col("o.O_CUSTKEY"),
            col("l.L_PARTKEY"),
            col("l.L_QUANTITY")
        )
    )

    print("\nrun_join_projection_tests Execution plan:")
    joined.explain(True)

    expected_columns = [
        "O_ORDERKEY",
        "O_ORDERKEY",
        "O_CUSTKEY",
        "L_PARTKEY",
        "L_QUANTITY"
    ]

    if joined.columns != expected_columns:
        issues.append(
            f"Join projection expected columns {expected_columns} but found {joined.columns}"
        )

    expected_rows = 13
    actual_rows = joined.count()

    if actual_rows != expected_rows:
        issues.append(
            f"Join projection expected {expected_rows} rows but found {actual_rows}"
        )

    first = joined.first()

    if first.O_CUSTKEY != 36901:
        issues.append(f"Join value expected 36901 rows but found {first.O_CUSTKEY}")
        print(f"Join value expected 36901 rows but found {first.O_CUSTKEY}")

    if first.L_PARTKEY != 155190:
        issues.append(f"Join value expected 155190 rows but found {first.L_PARTKEY}")
        print(f"Join value expected 155190 rows but found {first.L_PARTKEY}")

    status = "PASS" if not issues else "FAIL: " + " | ".join(issues)

    print(status)

def run_join_predicate_filter_on_child_tests(orders, lineitem, issues):
    print("\nrun_join_predicate_filter_on_child_tests")

    joined = (
        orders.alias("o")
        .join(
            lineitem.alias("l"),
            (col("o.O_ORDERKEY") == col("l.O_ORDERKEY"))
            & (col("l.L_QUANTITY") > 20),
            "inner",
        )
    )
    print("\nrun_join_predicate_filter_on_child_tests Execution plan:")
    joined.explain(True)

    actual = joined.count()
    expected = 10

    if actual != expected:
        issues.append(f"Join predicate filter on child expected {expected} rows but found {actual}")

def run_join_predicate_filter_on_parent_tests(orders, lineitem, issues):
    print("\nrun_join_predicate_filter_on_parent_tests")

    joined = (
        orders.alias("o")
        .join(
            lineitem.alias("l"),
            (col("o.O_ORDERKEY") == col("l.O_ORDERKEY"))
            & (col("o.O_ORDERPRIORITY") == "5-LOW"),
            "inner",
        )
    )
    print("\nrun_join_predicate_filter_on_parent_tests Execution plan:")
    joined.explain(True)

    actual = joined.count()
    expected = 12

    if actual != expected:
        issues.append(f"Join predicate filter on parent expected {expected} rows but found {actual}")

def run_join_predicate_filter_on_child_and_parent_tests(orders, lineitem, issues):
    print("\nrun_join_predicate_filter_on_child_and_parent_tests")

    joined = (
        orders.alias("o")
        .join(
            lineitem.alias("l"),
            (col("o.O_ORDERKEY") == col("l.O_ORDERKEY"))
            & (col("o.O_ORDERPRIORITY") == "5-LOW")
            & (col("l.L_QUANTITY") > 20),
           "inner",
        )
    )
    print("\nrun_join_predicate_filter_on_child_and_parent_tests Execution plan:")
    joined.explain(True)

    actual = joined.count()
    expected = 9

    if actual != expected:
        issues.append(f"Join predicate filter on parent and child expected {expected} rows but found {actual}")

def run_join_predicate_filter_on_parent_ambiguous_key_tests(orders, lineitem, issues):
    print("\nrun_join_predicate_filter_on_parent_ambiguous_key_tests")

    joined = (
        orders.alias("o")
        .join(
            lineitem.alias("l"),
            (col("o.O_ORDERKEY") == col("l.O_ORDERKEY"))
            & (col("o.O_ORDERKEY") == 1),
            "inner",
        )
    )
    print("\nrun_join_predicate_filter_on_parent_ambiguous_key_tests Execution plan:")
    joined.explain(True)

    actual = joined.count()
    expected = 6

    if actual != expected:
        issues.append(f"Join predicate filter on parent ambiguous key expected {expected} rows but found {actual}")

def run_join_predicate_filter_on_child_ambiguous_key_tests(orders, lineitem, issues):
    print("\nrun_join_predicate_filter_on_child_ambiguous_key_tests")

    joined = (
        orders.alias("o")
        .join(
            lineitem.alias("l"),
            (col("o.O_ORDERKEY") == col("l.O_ORDERKEY"))
            & (col("l.O_ORDERKEY") == 1),
            "inner",
        )
    )
    print("\nrun_join_predicate_filter_on_child_ambiguous_key_tests Execution plan:")
    joined.explain(True)

    actual = joined.count()
    expected = 6

    if actual != expected:
        issues.append(f"Join predicate filter on child ambiguous key expected {expected} rows but found {actual}")

def run_join_filter_tests(orders, lineitem, issues):
    print("\nrun_join_filter_tests")

    joined = (
        orders.alias("o")
        .join(
            lineitem.alias("l"),
            col("o.O_ORDERKEY") == col("l.O_ORDERKEY"),
            "inner"
        )
        .filter(col("L_QUANTITY") > "20")
    )

    print("\nrun_join_filter_tests Execution plan:")
    joined.explain(True)

    actual = joined.count()
    expected = 10

    if actual != expected:
        issues.append(f"Join filter expected {expected} rows but found {actual}")

def run_join_value_tests(orders, lineitem, issues):
    print("\nrun_join_value_tests")

    joined = (
        orders.alias("o")
        .join(
            lineitem.alias("l"),
            col("o.O_ORDERKEY") == col("l.O_ORDERKEY"),
            "inner"
        )
        .filter(col("o.O_ORDERKEY") == "1")
        .select(
            col("o.O_CUSTKEY"),
            col("l.L_PARTKEY"),
            col("l.L_LINENUMBER")
        )
    )

    print("\nrun_join_value_tests Execution plan:")
    joined.explain(True)

    actual = joined.count()
    expected = 6

    if actual != expected:
        issues.append(f"Join expected {expected} rows but found {actual}")

    first = joined.first()

    if first.O_CUSTKEY != 36901:
        issues.append(f"Join value expected 36901 rows but found {first.O_CUSTKEY}")
        print(f"Join value expected 36901 rows but found {first.O_CUSTKEY}")

    if first.L_PARTKEY != 155190:
        issues.append(f"Join value expected 155190 rows but found {first.L_PARTKEY}")
        print(f"Join value expected 155190 rows but found {first.L_PARTKEY}")

def run_left_join_tests(orders, lineitem, issues):
    print("\nrun_left_join_tests")

    joined = (
        orders.alias("o")
        .join(
            lineitem.alias("l"),
            col("o.O_ORDERKEY") == col("l.O_ORDERKEY"),
            "left"
        )
    )

    print("\nrun_left_join_tests Execution plan:")
    joined.explain(True)

    actual = joined.count()
    expected = 13

    if actual != expected:
        issues.append(
            f"Left join expected {expected} rows but found {actual}"
        )
        print(f"Left join expected {expected} rows but found {actual}")

def run_right_join_tests(orders, lineitem, issues):
    print("\nrun_right_join_tests")

    joined = (
        orders.alias("o")
        .join(
            lineitem.alias("l"),
            col("o.O_ORDERKEY") == col("l.O_ORDERKEY"),
            "right"
        )
    )

    print("\nrun_right_join_tests Execution plan:")
    joined.explain(True)

    actual = joined.count()
    expected = 13

    if actual != expected:
        issues.append(
            f"Right join expected {expected} rows but found {actual}"
        )
        print(f"Right join expected {expected} rows but found {actual}")

def run_simple_join_projection_tests(orders, lineitem, issues):
    print("\nrun_simple_join_projection_tests")

    joined = (
        orders.alias("o")
        .join(
            lineitem.alias("l"),
            col("o.O_ORDERKEY") == col("l.O_ORDERKEY"),
            "inner"
        )
        .select(
            col("o.O_ORDERKEY")
        )
    )

    print("\nrun_simple_join_projection_tests Execution plan:")
    joined.explain(True)

    expected_columns = [
        "O_ORDERKEY"
    ]

    if joined.columns != expected_columns:
        issues.append(
            f"Simple join projection expected columns {expected_columns} but found {joined.columns}"
        )

    expected_rows = 13
    actual_rows = joined.count()

    if actual_rows != expected_rows:
        issues.append(
            f"Simple join projection expected {expected_rows} rows but found {actual_rows}"
        )

    status = "PASS" if not issues else "FAIL: " + " | ".join(issues)

    print(status)


def run_join_on_uuid_projection_tests(uuid_parent, uuid_child, issues):
    print("\nrun_join_on_uuid_projection_tests")

    joined = (
        uuid_parent.alias("p")
        .join(
            uuid_child.alias("c"),
            col("p.PARENT_ID") == col("c.PARENT_ID"),
            "inner"
        )
        .select(
            col("p.PARENT_ID"),
            col("p.PARENT_NAME"),
            col("c.CHILD_ID"),
            col("c.CHILD_NAME")
        )
    )

    print("\nrun_join_on_uuid_projection_tests Execution plan:")
    joined.explain(True)

    expected_columns = [
        "PARENT_ID",
        "PARENT_NAME",
        "CHILD_ID",
        "CHILD_NAME"
    ]

    if joined.columns != expected_columns:
        issues.append(
            f"Join on uuid projection expected columns {expected_columns} but found {joined.columns}"
        )

    expected_rows = 3
    actual_rows = joined.count()

    if actual_rows != expected_rows:
        issues.append(
            f"Join on uuid projection expected {expected_rows} rows but found {actual_rows}"
        )

    status = "PASS" if not issues else "FAIL: " + " | ".join(issues)

    print(status)

def run_join_on_uuid_columns_and_uuid_and_literal_projection_tests(uuid_parent, uuid_child, issues):
    # Without SupportsPushdownV2Filter this only tests that we can join on UUID to UUID columns. Spark handled the filter instead.
    # SupportsPushdownV2Filter will allow this test, without modification,
    # to test that the connector can not only bind string parameters to STRING columns,
    # but bind strings parameters to UUID columns if the underlying Spanner column is UUID.
    print("\nrun_join_on_uuid_columns_and_uuid_and_literal_projection_tests")

    joined = (
        uuid_parent.alias("p")
        .join(
            uuid_child.alias("c"),
            (col("p.PARENT_ID") == col("c.PARENT_ID"))
            & (col("p.PARENT_ID") == "550e8400-e29b-41d4-a716-446655440000"),
            "inner"
        )
        .select(
            col("p.PARENT_ID"),
            col("p.PARENT_NAME"),
            col("c.CHILD_ID"),
            col("c.CHILD_NAME")
        )
    )

    print("\nrun_join_on_uuid_columns_and_uuid_and_literal_projection_tests Execution plan:")
    joined.explain(True)

    expected_columns = [
        "PARENT_ID",
        "PARENT_NAME",
        "CHILD_ID",
        "CHILD_NAME"
    ]

    if joined.columns != expected_columns:
        issues.append(
            f"Join on uuid columns and uuid and literal projection expected columns {expected_columns} but found {joined.columns}"
        )

    expected_rows = 2
    actual_rows = joined.count()

    if actual_rows != expected_rows:
        issues.append(
            f"Join on uuid columns and uuid and literal projection expected {expected_rows} rows but found {actual_rows}"
        )

    status = "PASS" if not issues else "FAIL: " + " | ".join(issues)

    print(status)

def run_join_on_uuid_columns_and_string_and_literal_projection_tests(uuid_parent, uuid_child, issues):
    # Without SupportsPushdownV2Filter this only tests that we can join on UUID to UUID columns. Spark handled the filter instead.
    # SupportsPushdownV2Filter will allow this test, without modification,
    # to test that the connector can not only bind string parameters to STRING columns,
    # but bind strings parameters to UUID columns if the underlying Spanner column is UUID.
    print("\nrun_join_on_uuid_columns_and_string_and_literal_projection_tests")

    joined = (
        uuid_parent.alias("p")
        .join(
            uuid_child.alias("c"),
            (col("p.PARENT_ID") == col("c.PARENT_ID"))
            & (col("p.PARENT_NAME") == "6ba7b810-9dad-11d1-80b4-00c04fd430c8"),
            "inner"
        )
        .select(
            col("p.PARENT_ID"),
            col("p.PARENT_NAME"),
            col("c.CHILD_ID"),
            col("c.CHILD_NAME")
        )
    )

    print("\nrun_join_on_uuid_columns_and_string_and_literal_projection_tests Execution plan:")
    joined.explain(True)

    expected_columns = [
        "PARENT_ID",
        "PARENT_NAME",
        "CHILD_ID",
        "CHILD_NAME"
    ]

    if joined.columns != expected_columns:
        issues.append(
            f"Join on uuid columns and string and literal projection expected columns {expected_columns} but found {joined.columns}"
        )

    expected_rows = 1
    actual_rows = joined.count()

    if actual_rows != expected_rows:
        issues.append(
            f"Join on uuid projection expected {expected_rows} rows but found {actual_rows}"
        )

    status = "PASS" if not issues else "FAIL: " + " | ".join(issues)

    print(status)

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
    spark.conf.set(
        "spark.sql.optimizer.datasourceV2JoinPushdown",
        "true"
    )
    print(
        spark.conf.get(
            "spark.sql.optimizer.datasourceV2JoinPushdown"
        )
    )

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
        "LINEITEM"
    )

    lineitem_indexed = load_table(
        spark,
        project_id,
        instance_id,
        database_id,
        "LINEITEM",
        indexHint="LineitemJoinTestIndex"
    )

    uuid_parent = load_table(
        spark,
        project_id,
        instance_id,
        database_id,
        "UUID_PARENT",
    )

    uuid_child = load_table(
        spark,
        project_id,
        instance_id,
        database_id,
        "UUID_CHILD",
    )

    print('The resulting schema are')
    print('ORDERS')
    orders.printSchema()
    print('LINEITEM')
    lineitem.printSchema()

    issues = []
    run_join_on_uuid_projection_tests(uuid_parent, uuid_child, issues)
    run_join_on_uuid_columns_and_uuid_and_literal_projection_tests(uuid_parent, uuid_child, issues)
    run_join_on_uuid_columns_and_string_and_literal_projection_tests(uuid_parent, uuid_child, issues)
    run_inner_join_tests(orders, lineitem, issues)
    run_join_projection_tests(orders, lineitem, issues)
    run_join_predicate_filter_on_child_tests(orders, lineitem, issues)
    run_join_predicate_filter_on_parent_tests(orders, lineitem, issues)
    run_join_predicate_filter_on_child_and_parent_tests(orders, lineitem, issues)
    run_join_predicate_filter_on_parent_ambiguous_key_tests(orders, lineitem, issues)
    run_join_predicate_filter_on_child_ambiguous_key_tests(orders, lineitem, issues)
    run_join_filter_tests(orders, lineitem, issues)
    run_join_value_tests(orders, lineitem, issues)
    run_left_join_tests(orders, lineitem, issues)
    run_right_join_tests(orders, lineitem, issues)
    run_simple_join_projection_tests(orders, lineitem_indexed, issues)

    write_results(spark, output_path, issues)

if __name__ == '__main__':
  main()
