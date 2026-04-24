package com.google.cloud.spark.spanner

import org.apache.spark.sql.SparkSession
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
    val tables = Seq("customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier")

    val spark = SparkSession.builder()
      .appName(s"TPC-H-Query-$queryNumber")
      .getOrCreate()

    val provider = s"com.google.cloud.spark.spanner.Spark${buildSparkVersion.replace(".", "")}SpannerTableProvider"

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

    println(s"Starting TPC-H Query $queryNumber...")
    val startTime = System.nanoTime()

    // Execute and force action with count()
    val resultCount = spark.sql(sqlQuery).count()

    val endTime = System.nanoTime()
    val durationSeconds = (endTime - startTime) / 1e9

    println(s"Query $queryNumber finished in $durationSeconds seconds. Result count: $resultCount")

    // Log results to GCS (Reuse your existing JSON logic here)
    saveResults(resultsBucket, queryNumber, durationSeconds, resultCount, spark)
  }

  // Helper to save JSON results similar to your Write benchmark
  private def saveResults(bucket: String, qNum: Int, duration: Double, count: Long, spark: SparkSession): Unit = {
    val runId = UUID.randomUUID().toString.take(8)
    val runTimestamp = java.time.format.DateTimeFormatter.ISO_INSTANT.format(java.time.Instant.now())
    val resultJson = Json.obj(
      "benchmarkName" -> "TPCH-Read",
      "queryNumber" -> qNum,
      "durationSeconds" -> duration,
      "rowCount" -> count,
      "timestamp" -> runTimestamp
    )
    val path = s"gs://$bucket/TPCH/Q${qNum}_${runId}.json"
    val fs = FileSystem.get(new URI(path), spark.sparkContext.hadoopConfiguration)
    val writer = new OutputStreamWriter(fs.create(new Path(path), true), StandardCharsets.UTF_8)
    writer.write(Json.prettyPrint(resultJson))
    writer.close()
  }
}
