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

import sys
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col

def main():
  # Ensure you are importing necessary modules inside your script or at the top
  spark = SparkSession.builder.appName('Read Acceptance Test on Spark - filter pushdown').getOrCreate()

  table = 'ATable'
  df = spark.read.format('cloud-spanner') \
      .option("projectId", sys.argv[2]) \
      .option("instanceId", sys.argv[3]) \
      .option("databaseId", sys.argv[4]) \
      .option("table", table) \
      .load()

  print('The resulting schema is:')
  df.printSchema()

  # Execute your filter operation
  rows = (
    df
    .filter(col("B") == "2")
    .collect()
  )

  # Initialize an empty array to track validation issues
  issues = []

  # 1. Replace assertEqual with a standard conditional check
  actual_row_count = len(rows)
  expected_row_count = 1

  if actual_row_count != expected_row_count:
    issues.append(f"Expected {expected_row_count} row, but found {actual_row_count}")

  # 2. Determine Final Status safely without crashing
  status_msg = "PASS" if not issues else "FAIL: " + " | ".join(issues)

  # 3. Create a DataFrame from the result string using Spark Row
  df_result = spark.createDataFrame([Row(summary=status_msg)])
  df_result.show(truncate=False)

  # 4. Coalesce and write results to the path provided in your first argument
  df_result.coalesce(1).write.mode("overwrite").csv(sys.argv[1])

if __name__ == "__main__":
    main()
