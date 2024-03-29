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
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main():
  spark = SparkSession.builder.appName('Acceptance Test on Spark').getOrCreate()

  table = 'ATable'
  df = spark.read.format('cloud-spanner') \
      .option("projectId", sys.argv[2]) \
      .option("instanceId", sys.argv[3]) \
      .option("databaseId", sys.argv[4]) \
      .option("table", table) \
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
