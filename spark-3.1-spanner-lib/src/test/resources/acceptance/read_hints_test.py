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


def load_table(spark, project_id, instance_id, database_id, table, hint):
    return (
        spark.read.format("cloud-spanner")
        .option("projectId", project_id)
        .option("instanceId", instance_id)
        .option("databaseId", database_id)
        .option("table", table)
        .option("indexHint", hint)
        .load()
    )

def run_index_hint_tests(df, issues):
    print("\nrun_index_hint_tests")

    df = df.select("A", "B", "D", "E")
    actual_count = df.filter(col("B") == "20").count()
    expected_count = 1

    if actual_count != expected_count:
        issue = (
            f"Index hint test: expected count {expected_count}, "
            f"but found {actual_count}"
        )
        issues.append(issue)

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
    spark = SparkSession.builder.appName('Read Acceptance Test on Spark - index hints').getOrCreate()

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
        "ATableByB"
    )

    print('The resulting schema is')
    df.printSchema()

    issues = []
    run_index_hint_tests(df, issues)
    write_results(spark, output_path, issues)

if __name__ == '__main__':
  main()
