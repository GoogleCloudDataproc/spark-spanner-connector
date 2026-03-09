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
from datetime import datetime, date
from decimal import Decimal
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, LongType, BinaryType, TimestampType, DecimalType, BooleanType, DoubleType, DateType

def main():

    # Initialize Spark Session
    spark = SparkSession.builder.appName('Write Acceptance Test on Spark').getOrCreate()

    # 1. Define the Schema (Column Name, Type, Nullable)
    schema = StructType([
        StructField("A", LongType(), False),
        StructField("B", StringType(), True),
        StructField("C", BinaryType(), True),
        StructField("D", TimestampType(), True),
        StructField("E", DecimalType(38, 9), True),
        StructField("F", BooleanType(), True),
        StructField("G", DoubleType(), True),
        StructField("H", DateType(), True)
    ])

    # 2. Prepare Data as a list of tuples
    data = [
        (1,  "2",  None, datetime.fromisoformat("2023-08-22T12:22:00"), Decimal("1000.282111401"), True, 123.456, date(2023, 12, 25)),
        (10, "20", None, datetime.fromisoformat("2023-08-22T12:23:00"), Decimal("10000.282111603"), False, 987.654, date(2023, 12, 24)),
        (30, "30", None, datetime.fromisoformat("2023-08-22T12:24:00"), Decimal("30000.282111805"), True, -2121.1212, date(2023, 12, 23))
    ]

    # 3. Create the DataFrame
    dfw = spark.createDataFrame(data, schema)
    dfw.show()

    table = 'AWriteTable'

    # Configure Spanner properties
    spanner_base_options = {
        "instanceId": sys.argv[3],
        "databaseId": sys.argv[4],
        "projectId": sys.argv[2],
        "table": table
    }

    spanner_write_options = {
        **spanner_base_options,
        "mutationType": "insert_or_update", # Use this to avoid ALREADY_EXISTS errors
        "enablePartialRowUpdates": "true"   # Required since not all columns are being populated
    }

    spanner_read_options = {
        **spanner_base_options
    }

    dfw.write.format('cloud-spanner') \
        .options(**spanner_write_options) \
        .mode("append") \
        .save()

    # Read the table to verify the write operation
    df = spark.read.format('cloud-spanner') \
      .options(**spanner_read_options) \
      .load(table)

    print('The resulting schema is')
    df.printSchema()
    df.show()

    df_result = verify_data_to_df(dfw, df, spark)
    df_result.show()

    # coalesce 1 to ensure results are written in single partition and avoid empty file creation.
    df_result.coalesce(1).write.csv(sys.argv[1])


def verify_data_to_df(df_expected, df_actual, spark):
    issues = []

    # 1. Validation Logic
    if df_expected.schema != df_actual.schema:
        issues.append("Schema mismatch")

    # Cache DFs for performance since we'll perform multiple actions.
    df_expected.cache()
    df_actual.cache()

    missing_rows_count = df_expected.subtract(df_actual).count()
    if missing_rows_count > 0:
        issues.append(f"Missing rows in actual: {missing_rows_count}")

    extra_rows_count = df_actual.subtract(df_expected).count()
    if extra_rows_count > 0:
        issues.append(f"Extra rows in actual: {extra_rows_count}")

    df_expected.unpersist()
    df_actual.unpersist()

    # 2. Determine Final Status
    status_msg = "PASS" if not issues else "FAIL: " + " | ".join(issues)

    # 3. Create a DataFrame from the result string
    return spark.createDataFrame([Row(summary=status_msg)])

if __name__ == '__main__':
  main()
