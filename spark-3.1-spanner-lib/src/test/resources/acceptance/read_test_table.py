#!/usr/bin/env python
# Copyright 2023 Google Inc. All Rights Reserved.
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
        .load()
    )

def run_sum_tests(df, issues):
    print("\nrun_sum_tests")

    df = df.select("A", "B", "D", "E")
    actual_count = df.groupBy().sum('A').first()[0]
    expected_count = 131

    if actual_count != expected_count:
        issue = (
            f"Sum test: expected count {expected_count}, "
            f"but found {actual_count}"
        )
        issues.append(issue)


def run_schema_tests(df, issues):
    print("\nrun_schema_tests")

    expected_schema = StructType([
        StructField('A', LongType(), False),
        StructField('B', StringType(), True),
        StructField('C', BinaryType(), True),
        StructField('D', TimestampType(), True),
        StructField('E', DecimalType(38, 9), True),
        StructField('F', BooleanType(), True),
        StructField('G', DoubleType(), True),
        StructField('H', DateType(), True),
        StructField('I', ArrayType(StringType(), True), True),
        StructField('J', StringType(), True),
        StructField('K', DoubleType(), True),
    ])

    schemas_equivalent(df.schema, expected_schema, issues)


def schemas_equivalent(actual_schema, expected_schema, issues):
    if len(actual_schema.fields) != len(expected_schema.fields):
        issues.append(
            f"Schema length mismatch. Expected={len(expected_schema.fields)}, Actual={len(actual_schema.fields)}"
        )

    for actual, expected in zip(actual_schema.fields, expected_schema.fields):
        if actual.name != expected.name:
            issues.append(
                f"Schema name mismatch. Expected={expected.name}, Actual={actual.name}"
            )
        if actual.dataType != expected.dataType:
            issues.append(
                f"Schema dataType mismatch. Expected={expected.dataType}, Actual={actual.dataType}"
            )
        if actual.nullable != expected.nullable:
            issues.append(
                f"Schema nullable mismatch. Expected={expected.nullable}, Actual={actual.nullable}"
            )


def run_type_mapping_tests(df, issues):
    print("\nrun_type_mapping_tests")

    row = df.filter(col("A") == 40).first()

    # Verify values
    if row.A != 40:
        issues.append(f"A expected 1 but found {row.A}")

    if row.B != "40":
        issues.append(f"B expected abc but found {row.B}")

    if row.C != bytearray(b"xyz") and row.C != b"xyz":
        issues.append(f"C expected xyz bytes but found {row.C}")

    if row.E != Decimal("123.456789123"):
        issues.append(f"E expected 123.456789123 but found {row.E}")

    if row.F is not True:
        issues.append(f"F expected True but found {row.F}")

    if row.G != 3.14:
        issues.append(f"G expected 3.14 but found {row.G}")

    # Verify types
    if not isinstance(row.A, int):
        issues.append(f"A expected int but found {type(row.A)}")

    if not isinstance(row.E, Decimal):
        issues.append(f"E expected Decimal but found {type(row.E)}")

    if not isinstance(row.H, date):
        issues.append(f"H expected date but found {type(row.H)}")

    if not isinstance(row.D, datetime):
        issues.append(f"D expected datetime but found {type(row.D)}")

    if not isinstance(row.I, list):
        issues.append(f"I expected list but found {type(row.I)}")


def run_null_tests(df, issues):
    print("\nrun_null_tests")

    row = df.filter(col("A") == 50).first()

    if row.B is not None:
        issues.append("B expected NULL")

    if row.C is not None:
        issues.append("C expected NULL")

    if row.D is not None:
        issues.append("D expected NULL")

    if row.E is not None:
        issues.append("E expected NULL")

    if row.F is not None:
        issues.append("F expected NULL")

    if row.G is not None:
        issues.append("G expected NULL")

    if row.H is not None:
        issues.append("H expected NULL")

    if row.I is not None:
        issues.append("I expected NULL")

    if row.J is not None:
        issues.append("J expected NULL")

    if row.K is not None:
        issues.append("K expected NULL")


def run_json_tests(df, issues):
    print("\nrun_json_tests")

    expected = {"name":"john"}
    row = df.filter(col("A") == 40).first()
    actual = json.loads(row.J)

    if actual != expected:
        issues.append(f"J expected {expected} ")

def run_array_tests(df, issues):
    print("\nrun_array_tests")

    row = df.filter(col("A") == 40).first()

    if row.I != ["a", "b", "c"]:
        issues.append(
            f"I expected ['a','b','c'] but found {row.I}"
        )

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
    spark = SparkSession.builder.appName('Read Acceptance Test on Spark - table load').getOrCreate()

    output_path = sys.argv[1]
    project_id = sys.argv[2]
    instance_id = sys.argv[3]
    database_id = sys.argv[4]

    df = load_table(
        spark,
        project_id,
        instance_id,
        database_id,
        "ATable",
    )

    print('The resulting schema is')
    df.printSchema()

    issues = []
    run_sum_tests(df, issues)
    run_schema_tests(df, issues)
    run_type_mapping_tests(df, issues)
    run_null_tests(df, issues)
    run_json_tests(df, issues)
    run_array_tests(df, issues)
    write_results(spark, output_path, issues)

if __name__ == '__main__':
  main()
