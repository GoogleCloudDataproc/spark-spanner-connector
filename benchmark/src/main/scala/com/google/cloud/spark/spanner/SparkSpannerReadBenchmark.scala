package com.google.cloud.spark.spanner

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, round, trim}
import org.apache.spark.sql.types._
import play.api.libs.json.{JsValue, Json}
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.OutputStreamWriter
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.UUID

object SparkSpannerReadBenchmark {
  def main(args: Array[String]): Unit = {
    val configStr = args(0)
    val config: JsValue = Json.parse(configStr)

    val projectId = (config \ "projectId").as[String]
    val instanceId = (config \ "instanceId").as[String]
    val databaseId = (config \ "databaseId").as[String]
    val resultsBucket = (config \ "resultsBucket").as[String]
    val buildSparkVersion = (config \ "buildSparkVersion").as[String]

    // TPC-H specific: Which query number to run (1-22) and the table list
    val queryNumber = (config \ "tpcQueryNumber").as[Int]
    val tables = (config \ "tpchTables").as[Seq[String]]

    val spark = SparkSession.builder()
      .appName(s"TPC-H-Query-$queryNumber")
      .getOrCreate()

    val provider = SpannerScalaUtils.getProviderClassName(buildSparkVersion)

    // Register all TPC-H tables as Temp Views
    tables.foreach { tableName =>
      spark.read
        .format(provider)
        .option("projectId", projectId)
        .option("instanceId", instanceId)
        .option("databaseId", databaseId)
        .option("table", tableName)
        .load()
        .createOrReplaceTempView(tableName)
    }

    // Get the SQL string (you can store these in a Map or external files)
    val sqlQuery = TPCHQueries.getQuery(queryNumber)
    val actualDf = spark.sql(sqlQuery)

    actualDf.cache()

    println(s"Starting TPC-H Query $queryNumber...")
    val startTime = System.nanoTime()
    actualDf.cache()

    // Execute and force action with count()
    val actualRows: Array[org.apache.spark.sql.Row] = actualDf.collect()

    val endTime = System.nanoTime()
    val durationSeconds = (endTime - startTime) / 1e9
    val resultCount = actualRows.length

    println(s"Query $queryNumber finished in $durationSeconds seconds. Result count: $resultCount")

    // Convert the collected rows back to a DF for the validator
    // We use the original schema from actualDf to keep it consistent
    val actualResultDf = spark.createDataFrame(
      spark.sparkContext.parallelize(actualRows),
      actualDf.schema
    )

    // 3. CALL THE VALIDATION
    val isValid = validateQueryOutput(actualResultDf, queryNumber, resultsBucket, spark)

    // Log results to GCS (Reuse your existing JSON logic here)
    saveResults(resultsBucket, queryNumber, durationSeconds, resultCount, isValid, spark)
  }

  // Helper to save JSON results similar to your Write benchmark
  private def saveResults(bucket: String, qNum: Int, duration: Double, count: Long, isValid: Boolean, spark: SparkSession): Unit = {
    val runId = UUID.randomUUID().toString.take(8)
    val runTimestamp = java.time.format.DateTimeFormatter.ISO_INSTANT.format(java.time.Instant.now())
    val resultJson = Json.obj(
      "benchmarkName" -> "TPCH-Read",
      "queryNumber" -> qNum,
      "durationSeconds" -> duration,
      "rowCount" -> count,
      "timestamp" -> runTimestamp,
      "isValid" -> isValid
    )
    val path = s"gs://$bucket/TPCH/Q${qNum}_${runId}.json"
    val fs = FileSystem.get(new URI(path), spark.sparkContext.hadoopConfiguration)
    val writer = new OutputStreamWriter(fs.create(new Path(path), true), StandardCharsets.UTF_8)
    writer.write(Json.prettyPrint(resultJson))
    writer.close()
  }

  def normalize(df: DataFrame): DataFrame = {
    val normalizedCols = df.schema.fields.map { field =>
      field.dataType match {
        // Round all floating point/decimal types to 2 decimal places
        // We cast to Decimal(38,2) to ensure a consistent internal representation
        case _: DoubleType | _: FloatType | _: DecimalType =>
          round(col(field.name), 2).cast(DecimalType(38, 2)).as(field.name)

        // Trim strings to remove padding found in .out files
        case StringType =>
          trim(col(field.name)).as(field.name)

        // Keep Longs (counts) and other types as they are
        case _ =>
          col(field.name)
      }
    }
    df.select(normalizedCols: _*)
  }

  def validateQueryOutput(
                           actualDf: DataFrame,
                           queryNum: Int,
                           referenceBucket: String,
                           spark: SparkSession
                         ): Boolean = {

    // 1. Load the reference .out file from GCS
    // Note: TPC-H files use '|' as a separator
    val referencePath = s"gs://$referenceBucket/answers/q$queryNum.out"

    val expectedDf = spark.read
      .option("sep", "|")
      .option("header", "true")              // Use the header in the file
      .option("inferSchema", "true")
      .option("ignoreLeadingWhiteSpace", "true")  // Clean up those padded numbers
      .option("ignoreTrailingWhiteSpace", "true")
      .csv(referencePath)

    // 2. Alignment
    // Ensure both DFs have identical column names and sort order for comparison
    val actualNormalized = normalize(actualDf)
    val expectedNormalized = normalize(expectedDf)

    // Sorting after normalization is safer
    val actualSorted = actualNormalized.sort(actualNormalized.columns.map(col): _*)
    val expectedSorted = expectedNormalized.sort(actualNormalized.columns.map(col): _*)

    // 3. Comparison
    // exceptAll returns rows in one DF but not the other

    // 3.1. Calculate the differences once
    val actualExtra = actualSorted.except(expectedSorted)
    val expectedExtra = expectedSorted.except(actualSorted)

    // 3.2. Cache them or collect them to avoid re-computation
    val actualMismatches = actualExtra.collect()
    val expectedMismatches = expectedExtra.collect()

    val totalMismatches = actualMismatches.length + expectedMismatches.length

    if (totalMismatches > 0) {
      println(s"Query $queryNum: FAILED ($totalMismatches total row mismatches found)")

      if (actualMismatches.nonEmpty) {
        println("Rows in Result but NOT in Reference:")
        actualExtra.show(false) // This is now cheap because the data is likely in memory
      }

      if (expectedMismatches.nonEmpty) {
        println("Rows in Reference but NOT in Result:")
        expectedExtra.show(false)
      }
      false
    } else {
      println(s"Query $queryNum: PASSED")
      true
    }
  }
}
