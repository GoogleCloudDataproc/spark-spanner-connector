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
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime, date
from decimal import Decimal
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, BinaryType, TimestampType, DecimalType, BooleanType, DoubleType, DateType

def main():

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Initialize Spark Session
    spark = SparkSession.builder.appName('Write Acceptance Test on Spark').getOrCreate()

    # 1. Define the Schema (Column Name, Type, Nullable)
    logging.info('writeTest step 1')
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
    logging.info('writeTest step 2')
    data = [
        (1,  "2",  None, datetime.fromisoformat("2023-08-22T12:22:00"), Decimal("1000.282111401"), True, 123.456, date(2023, 12, 25)),
        (10, "20", None, datetime.fromisoformat("2023-08-22T12:23:00"), Decimal("10000.282111603"), False, 987.654, date(2023, 12, 24)),
        (30, "30", None, datetime.fromisoformat("2023-08-22T12:24:00"), Decimal("30000.282111805"), True, -2121.1212, date(2023, 12, 23))
    ]

    # 3. Create the DataFrame
    logging.info('writeTest step 3')
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

    logging.info('writeTest write')

    new_dfw = dfw.write.format('cloud-spanner') \
        .options(**spanner_write_options) \
        .mode("append") \
        .save()

    # Read the table to verify the write operation
    logging.info('writeTest read')
    df = spark.read.format('cloud-spanner') \
      .options(**spanner_read_options) \
      .load(table)

    print('The resulting schema is')
    df.printSchema()

    df = df.select("A", "B", "D", "E")
    df = df.groupBy().sum('A')

    print('Table:')
    df.show()

    df.write.csv(sys.argv[1])

if __name__ == '__main__':
  main()
