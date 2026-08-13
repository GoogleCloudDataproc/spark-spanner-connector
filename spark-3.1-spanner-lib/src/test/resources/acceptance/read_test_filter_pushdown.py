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

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col
import sys


def load_table(spark, project_id, instance_id, database_id, table):
    return (
        spark.read.format("cloud-spanner")
        .option("projectId", project_id)
        .option("instanceId", instance_id)
        .option("databaseId", database_id)
        .option("table", table)
        .load()
    )


def validate_row_count(df, description, predicate, expected_count):
    actual_count = df.filter(predicate).count()

    if actual_count != expected_count:
        return (
            f"{description}: expected {expected_count} rows, "
            f"but found {actual_count}"
        )

    return None


def run_tests(df):
    print("run_tests")

    tests = [
        ("Filter equals", col("B") == "2", 1),
        ("Filter greater than", col("B") > "2", 3),
        ("Filter less than", col("B") < "40", 3),
        ("Filter greater than equals", col("G") >= 0.1, 3),
        ("Filter less than equals", col("G") <= 0.2, 3),
        ("Filter is null safe", col("G").eqNullSafe(col("K")), 2),
        ("Filter is null", col("C").isNull(), 4),
        ("Filter is not null", col("K").isNotNull(), 2),
        ("Filter in", col("B").isin("2","20"), 2),
        ("Filter and", (col("B") > "2") & (col("G") < 0.2), 1),
        ("Filter or", (col("B") == "2") | (col("B") == "30"), 2),
        ("Filter not", ~(col("B") == "2"), 3),
        ("Filter starts with", col("B").startswith("3"), 1),
        ("Filter ends with", col("B").endswith("0"), 3),
        ("Filter contains", col("B").contains("2"), 2),
   ]

    issues = []

    for description, predicate, expected_count in tests:
        print(f"\nRunning {description}")
        issue = validate_row_count(
            df, description, predicate, expected_count
        )

        if issue:
            issues.append(issue)

    return issues


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
    print("\n\nRead Acceptance Test - Filter Pushdown\n\n")

    spark = (
        SparkSession.builder
        .appName("Read Acceptance Test - Filter Pushdown")
        .getOrCreate()
    )

    print("spark.version: ", spark.version)

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

    print('The resulting schema is:')
    df.printSchema()

    issues = run_tests(df)

    write_results(spark, output_path, issues)


if __name__ == "__main__":
    main()