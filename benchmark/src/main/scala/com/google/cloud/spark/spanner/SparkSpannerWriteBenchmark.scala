package com.google.cloud.spark.spanner

import org.apache.spark.util.SizeEstimator
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{coalesce, col, current_timestamp, lit, udf}
import org.apache.spark.sql.{SaveMode, SparkSession}
import play.api.libs.json.{JsValue, Json}

import java.io.OutputStreamWriter
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.UUID

object SparkSpannerWriteBenchmark {
  private val payloadSample =
    """
      |{
      |  "data": [
      |    {"id": 1, "value": "a_long_string_to_make_the_file_bigger_01"},
      |    {"id": 2, "value": "a_long_string_to_make_the_file_bigger_02"},
      |    {"id": 3, "value": "a_long_string_to_make_the_file_bigger_03"},
      |    {"id": 4, "value": "a_long_string_to_make_the_file_bigger_04"},
      |    {"id": 5, "value": "a_long_string_to_make_the_file_bigger_05"},
      |    {"id": 6, "value": "a_long_string_to_make_the_file_bigger_06"},
      |    {"id": 7, "value": "a_long_string_to_make_the_file_bigger_07"},
      |    {"id": 8, "value": "a_long_string_to_make_the_file_bigger_08"},
      |    {"id": 9, "value": "a_long_string_to_make_the_file_bigger_09"},
      |    {"id": 10, "value": "a_long_string_to_make_the_file_bigger_10"},
      |    {"id": 11, "value": "a_long_string_to_make_the_file_bigger_11"},
      |    {"id": 12, "value": "a_long_string_to_make_the_file_bigger_12"},
      |    {"id": 13, "value": "a_long_string_to_make_the_file_bigger_13"},
      |    {"id": 14, "value": "a_long_string_to_make_the_file_bigger_14"},
      |    {"id": 15, "value": "a_long_string_to_make_the_file_bigger_15"},
      |    {"id": 16, "value": "a_long_string_to_make_the_file_bigger_16"}
      |  ]
      |}
      |""".stripMargin

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage: SparkSpannerBenchmark <benchmark-config-json>")
      sys.exit(1)
    }
    val configStr = args(0)
    val config: JsValue = Json.parse(configStr)

    val numRecords = (config \ "numRecords").as[Long]
    val writeTable = (config \ "writeTable").as[String]
    val databaseId = (config \ "databaseId").as[String]
    val instanceId = (config \ "instanceId").as[String]
    val projectId = (config \ "projectId").as[String]
    val resultsBucket = (config \ "resultsBucket").as[String]
    val buildSparkVersion = (config \ "buildSparkVersion").as[String]

    val mutationsPerTransaction = (config \ "mutationsPerTransaction").asOpt[Int].getOrElse(5000)
    val bytesPerTransaction = (config \ "bytesPerTransaction").asOpt[Long].getOrElse(3 * 1024 * 1024L)
    val numWriteThreads = (config \ "numWriteThreads").asOpt[Int].getOrElse(4)
    val maxPendingTransactions = (config \ "maxPendingTransactions").asOpt[Int].getOrElse(5)
    val assumeIdempotentRows = (config \ "assumeIdempotentRows").asOpt[Boolean].getOrElse(true)
    
    val defaultPartitions = 5 * 4 * 2 // workerCount * coreCount * 2
    val numPartitions = (config \ "numPartitions").asOpt[Int].getOrElse(defaultPartitions)

    val spark = SparkSession.builder().appName("SparkSpannerWriteBenchmark").getOrCreate()

    val generateUUID = udf(() => UUID.randomUUID().toString)

    println("Test: Writing data with new schema...")

    val dfWrite = spark
      .range(numRecords)
      .select(
        coalesce(generateUUID(), lit("0")).as("id"),
        lit(payloadSample).as("json_payload"),
        lit("short_label_constant").as("short_label"),
        lit(UUID.nameUUIDFromBytes("related_guid_constant".getBytes).toString).as("related_guid"),
        lit("A").as("status_flag"),
        current_timestamp().as("created_at"),
        current_timestamp().as("updated_at")
      )
      .cache() // Cache the DataFrame for size estimation and the write operation

    // Dynamically calculate the average row size
    val averageRowSizeBytes = SizeEstimator.estimate(dfWrite.first())
    val sizeInBytes = averageRowSizeBytes * numRecords
    val sizeMb = sizeInBytes / (1024 * 1024)
    println(s"Estimated job size: $sizeMb MB")
    println(f"Average row size: $averageRowSizeBytes bytes")
    println(s"Number of partitions: $numPartitions")

    val dfPartitioned = dfWrite.repartition(numPartitions).sortWithinPartitions(col("id"))

    println(s"Beginning write to table '$writeTable' with mutationsPerTransaction: $mutationsPerTransaction")
    val startTime = System.nanoTime()
    val provider = s"com.google.cloud.spark.spanner.Spark${buildSparkVersion.replace(".", "")}SpannerTableProvider"
    dfPartitioned
      .write
      .format(provider)
      .option("mutationsPerTransaction", mutationsPerTransaction)
      .option("bytesPerTransaction", bytesPerTransaction.toString)
      .option("projectId", projectId)
      .option("instanceId", instanceId)
      .option("databaseId", databaseId)
      .option("numWriteThreads", numWriteThreads.toString)
      .option("assumeIdempotentRows", assumeIdempotentRows.toString)
      .option("maxPendingTransactions", maxPendingTransactions.toString)
      .option("table", writeTable)
      .mode(SaveMode.Append)
      .save()
    dfWrite.unpersist() // Release the cached DataFrame
    val endTime = System.nanoTime()
    val durationSeconds = (endTime - startTime) / 1e9
    val throughput = sizeMb / durationSeconds

    println("Ending write")
    println(s"Write operation took: $durationSeconds seconds")
    println(f"Throughput: $throughput%.2f MB/s")

    val runId = UUID.randomUUID().toString.take(8)
    val runTimestamp = java.time.format.DateTimeFormatter.ISO_INSTANT.format(java.time.Instant.now())
    val sparkVersion = spark.version
    val connectorVersion = "0.1.0"

    val resultJson = Json.obj(
      "runId" -> runId,
      "runTimestamp" -> runTimestamp,
      "benchmarkName" -> "SparkSpannerWriteBenchmark",
      "clusterSparkVersion" -> sparkVersion,
      "connectorVersion" -> connectorVersion,
      "spannerConfig" -> Json.obj(
        "projectId" -> projectId,
        "instanceId" -> instanceId,
        "databaseId" -> databaseId,
        "table" -> writeTable
      ),
      "benchmarkParameters" -> Json.obj(
        "numRecords" -> numRecords,
        "mutationsPerTransaction" -> mutationsPerTransaction,
        "bytesPerTransaction" -> bytesPerTransaction,
        "numWriteThreads" -> numWriteThreads,
        "maxPendingTransactions" -> maxPendingTransactions,
        "assumeIdempotentRows" -> assumeIdempotentRows
      ),
      "performanceMetrics" -> Json.obj(
        "durationSeconds" -> durationSeconds,
        "throughputMbPerSec" -> throughput,
        "totalSizeMb" -> sizeMb,
        "recordCount" -> numRecords
      ),
      "sparkConfig" -> Json.obj(
        "numPartitions" -> numPartitions
      )
    )

    val resultsPath = s"gs://$resultsBucket/SparkSpannerWriteBenchmark/${runTimestamp}_$runId.json"
    println(s"Writing results to $resultsPath")

    val resultsURI = new URI(resultsPath)
    val fs = FileSystem.get(resultsURI, spark.sparkContext.hadoopConfiguration)
    val outputPath = new Path(resultsPath)
    val os = fs.create(outputPath, true) // true to overwrite
    val writer = new OutputStreamWriter(os, StandardCharsets.UTF_8)
    try {
      writer.write(Json.prettyPrint(resultJson))
    } finally {
      writer.close()
      os.close()
    }

    println("Finished writing results.")
  }
}
