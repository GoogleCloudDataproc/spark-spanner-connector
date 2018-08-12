package spanner.spark

import org.apache.spark.sql.SaveMode

class SpannerSpec extends BaseSpec {

  "Cloud Spanner Connector" should "save DataFrame to Cloud Spanner table (default save mode)" in
    withSparkSession { spark =>

      val primaryKey = "id"
      val writeOpts = Map(
        SpannerOptions.INSTANCE_ID -> "dev-instance",
        SpannerOptions.DATABASE_ID -> "demo",
        SpannerOptions.TABLE -> s"scalatest_${System.currentTimeMillis()}",
        SpannerOptions.PRIMARY_KEY -> primaryKey
      )
      val q = spark.range(1)
      q.write
        .format("spanner")
        .options(writeOpts)
        .save
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
        .format("spanner")
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
          .format("spanner")
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
          .format("spanner")
          .options(writeOpts)
          .mode(SaveMode.Overwrite)
          .save

        val rowsSavedCount = spark.read.format("spanner").options(writeOpts).load.count
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
          .format("spanner")
          .options(writeOpts)
          .mode(SaveMode.ErrorIfExists)
          .save

        val evens = spark.range(2, 10, 2, numPartitions = 2)
        rowsToSaveCount = rowsToSaveCount + evens.count
        // Step 2. Append the even numbers
        evens.write
          .format("spanner")
          .options(writeOpts)
          .mode(SaveMode.Append)
          .save

        // Step 3. Make sure all the rows were saved correctly
        val rowsSavedCount = spark.read.format("spanner").options(writeOpts).load.count
        rowsSavedCount should be (rowsToSaveCount)
      } finally {
        withSpanner { spanner =>
          dropTables(spanner, instance, database, table)
        }
      }
    }
}
