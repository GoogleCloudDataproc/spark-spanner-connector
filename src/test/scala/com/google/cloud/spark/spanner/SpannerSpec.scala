/*
 * Copyright 2018 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.spanner

import org.apache.spark.sql.SaveMode

class SpannerSpec extends BaseSpec {

  "Cloud Spanner Connector" should "load data from a Spanner table" in
    withSparkSession { spark =>

      val instance = "dev-instance"
      val database = "demo"
      val primaryKey = "id"
      val table = s"scalatest_${System.currentTimeMillis()}"
      try {
        val schema = "id INT64, name STRING(MAX)"
        createTable(instance, database, table, schema, primaryKey)

        val opts = Map(
          SpannerOptions.INSTANCE_ID -> instance,
          SpannerOptions.DATABASE_ID -> database,
          SpannerOptions.TABLE -> table,
          SpannerOptions.PRIMARY_KEY -> primaryKey
        )
        val q = spark
          .read
          .format("cloud-spanner")
          .options(opts)
          .load
        q.rdd.getNumPartitions should be >= 1
      } finally {
        withSpanner { spanner =>
          dropTables(spanner, instance, database, table)
        }
      }
    }

  it should "load data from a Spanner table with 5 RDD partitions" in
    withSparkSession { spark =>

      val instance = "dev-instance"
      val database = "demo"
      val primaryKey = "id"
      val table = s"scalatest_${System.currentTimeMillis()}"
      val maxPartitions = 5
      try {
        val schema = "id INT64, name STRING(MAX)"
        createTable(instance, database, table, schema, primaryKey)

        val opts = Map(
          SpannerOptions.INSTANCE_ID -> instance,
          SpannerOptions.DATABASE_ID -> database,
          SpannerOptions.TABLE -> table,
          SpannerOptions.PRIMARY_KEY -> primaryKey,
          SpannerOptions.MAX_PARTITIONS -> maxPartitions.toString
        )
        val q = spark
          .read
          .format("cloud-spanner")
          .options(opts)
          .load
        q.rdd.getNumPartitions should be <= maxPartitions
      } finally {
        withSpanner { spanner =>
          dropTables(spanner, instance, database, table)
        }
      }
    }

  it should "save DataFrame to Cloud Spanner table (default save mode)" in
    withSparkSession { spark =>

      val instance = "dev-instance"
      val database = "demo"
      val primaryKey = "id"
      val table = s"scalatest_${System.currentTimeMillis()}"

      val writeOpts = Map(
        SpannerOptions.INSTANCE_ID -> instance,
        SpannerOptions.DATABASE_ID -> database,
        SpannerOptions.TABLE -> table,
        SpannerOptions.PRIMARY_KEY -> primaryKey
      )
      try {
        spark
          .range(1)
          .write
          .format("cloud-spanner")
          .options(writeOpts)
          .save
      } finally {
        withSpanner { spanner =>
          dropTables(spanner, instance, database, table)
        }
      }
    }

  it should "save DataFrame with custom schema" in
    withSparkSession { spark =>

      val schema = "A INT64, B STRING(100) NOT NULL"
      val primaryKey = "A DESC"
      val writeOpts = Map(
        SpannerOptions.INSTANCE_ID -> "dev-instance",
        SpannerOptions.DATABASE_ID -> "demo",
        SpannerOptions.TABLE -> s"scalatest_${System.currentTimeMillis()}",
        SpannerOptions.WRITE_SCHEMA -> schema,
        SpannerOptions.PRIMARY_KEY -> primaryKey
      )

      import spark.implicits._
      val q = Seq[(Long, String)]((1, "one"), (2, "two")).toDF("A", "B")
      q.write
        .format("cloud-spanner")
        .options(writeOpts)
        .save
    }

  it should "throw IllegalStateException when writing to a table that exists with ErrorIfExists" in
    withSparkSession { spark =>

      val instance = "dev-instance"
      val database = "demo"
      val primaryKey = "id"
      val tableName = s"scalatest_ErrorIfExists_${System.currentTimeMillis()}"
      val writeOpts = Map(
        SpannerOptions.INSTANCE_ID -> instance,
        SpannerOptions.DATABASE_ID -> database,
        SpannerOptions.TABLE -> tableName,
        SpannerOptions.PRIMARY_KEY -> primaryKey
      )

      val q = spark.range(1)

      if (!isTableAvailable(instance, database, tableName)) {
        val schema = toSpannerDDL(q.schema)
        createTable(instance, database, tableName, schema, primaryKey)
      }

      val thrown = the [IllegalStateException] thrownBy {
        q.write
          .format("cloud-spanner")
          .options(writeOpts)
          .mode(SaveMode.ErrorIfExists)
          .save
      }
      thrown.getMessage should startWith (s"Table '$tableName' already exists")
    }

  // FIXME Takes almost 3 mins
  it should "overwrite a table when writing to a table that exists with Overwrite" in
    withSparkSession { spark =>

      val instance = "dev-instance"
      val database = "demo"
      val table = s"scalatest_Overwrite_${System.currentTimeMillis()}"
      try {
        val primaryKey = "id"
        val writeOpts = Map(
          SpannerOptions.INSTANCE_ID -> instance,
          SpannerOptions.DATABASE_ID -> database,
          SpannerOptions.TABLE -> table,
          SpannerOptions.PRIMARY_KEY -> primaryKey
        )

        val rowsToSaveCount = 2
        val q = spark.range(0, rowsToSaveCount, 1, numPartitions = 2)

        if (!isTableAvailable(instance, database, table)) {
          val schema = toSpannerDDL(q.schema)
          createTable(instance, database, table, schema, primaryKey)
        }
        isTableAvailable(instance, database, table) should be(true)

        q.write
          .format("cloud-spanner")
          .options(writeOpts)
          .mode(SaveMode.Overwrite)
          .save

        val rowsSavedCount = spark.read.format("cloud-spanner").options(writeOpts).load.count
        rowsSavedCount should be (rowsToSaveCount)
      } finally {
        withSpanner { spanner =>
          dropTables(spanner, instance, database, table)
        }
      }
    }

  it should "append rows when writing to a table that exists (Append save mode)" in
    withSparkSession { spark =>

      val instance = "dev-instance"
      val database = "demo"
      val table = s"scalatest_Overwrite_${System.currentTimeMillis()}"
      try {
        val primaryKey = "id"
        val writeOpts = Map(
          SpannerOptions.INSTANCE_ID -> instance,
          SpannerOptions.DATABASE_ID -> database,
          SpannerOptions.TABLE -> table,
          SpannerOptions.PRIMARY_KEY -> primaryKey
        )

        var rowsToSaveCount = 0L

        val q1 = spark.range(0, 2, 1, numPartitions = 2)
        rowsToSaveCount = rowsToSaveCount + q1.count
        // Step 1. That creates the table with 2 rows
        q1.write
          .format("cloud-spanner")
          .options(writeOpts)
          .mode(SaveMode.ErrorIfExists)
          .save

        val evens = spark.range(2, 10, 2, numPartitions = 2)
        rowsToSaveCount = rowsToSaveCount + evens.count
        // Step 2. Append the even numbers
        evens.write
          .format("cloud-spanner")
          .options(writeOpts)
          .mode(SaveMode.Append)
          .save

        // Step 3. Make sure all the rows were saved correctly
        val rowsSavedCount = spark.read.format("cloud-spanner").options(writeOpts).load.count
        rowsSavedCount should be (rowsToSaveCount)
      } finally {
        withSpanner { spanner =>
          dropTables(spanner, instance, database, table)
        }
      }
    }

  it should "throw an IllegalArgumentException for Array of Arrays" in {
    withSparkSession { spark =>
      import spark.implicits._
      val q = Seq(Tuple1(Array(Array(1)))).toDF("arrays")
      val catalystType = q.schema.head.dataType
      val thrown = the [IllegalArgumentException] thrownBy toSpannerType(catalystType)
      thrown.getMessage should include ("ARRAY<ARRAY<INT>> is not supported in Cloud Spanner.")
    }
  }

  it should "insert into table (Overwrite save mode)" in {
    withSparkSession { spark =>
      val instance = "dev-instance"
      val database = "demo"
      val table = s"scalatest_insert_${System.currentTimeMillis()}"
      try {
        val primaryKey = "id"
        val writeOpts = Map(
          SpannerOptions.INSTANCE_ID -> instance,
          SpannerOptions.DATABASE_ID -> database,
          SpannerOptions.TABLE -> table,
          SpannerOptions.PRIMARY_KEY -> primaryKey
        )

        val rowsToSaveCount = 10

        import SpannerRelationProvider.{shortName => cloudSpanner}
        spark.range(rowsToSaveCount)
          .write
          .format(cloudSpanner)
          .options(writeOpts)
          .mode(SaveMode.ErrorIfExists)
          .saveAsTable(table)

        spark.range(rowsToSaveCount)
          .write
          .format(cloudSpanner)
          .options(writeOpts)
          .mode(SaveMode.Overwrite)
          .insertInto(table)

        val rowCount = spark.read.format(cloudSpanner).options(writeOpts).load.count
        rowCount should be (rowsToSaveCount)
      } finally {
        withSpanner { spanner =>
          dropTables(spanner, instance, database, table)
        }
      }
    }
  }

  it should "insert into table (Append save mode)" in {
    withSparkSession { spark =>
      val instance = "dev-instance"
      val database = "demo"
      val table = s"scalatest_insert_${System.currentTimeMillis()}"
      try {
        val primaryKey = "id"
        val writeOpts = Map(
          SpannerOptions.INSTANCE_ID -> instance,
          SpannerOptions.DATABASE_ID -> database,
          SpannerOptions.TABLE -> table,
          SpannerOptions.PRIMARY_KEY -> primaryKey
        )

        val firstCount = 10
        val totalRows = 20

        import SpannerRelationProvider.{shortName => cloudSpanner}
        spark.range(firstCount)
          .write
          .format(cloudSpanner)
          .options(writeOpts)
          .mode(SaveMode.ErrorIfExists)
          .saveAsTable(table)

        spark.range(firstCount, totalRows, 1)
          .write
          .format(cloudSpanner)
          .options(writeOpts)
          .mode(SaveMode.Append)
          .insertInto(table)

        val rowCount = spark.read.format(cloudSpanner).options(writeOpts).load.count
        rowCount should be (totalRows)
      } finally {
        withSpanner { spanner =>
          dropTables(spanner, instance, database, table)
        }
      }
    }
  }
}
