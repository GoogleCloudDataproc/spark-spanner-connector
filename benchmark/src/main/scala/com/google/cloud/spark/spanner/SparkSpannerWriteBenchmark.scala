package com.google.cloud.spark.spanner

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{coalesce, col, current_timestamp, lit, udf}
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.io.OutputStreamWriter
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.util.Try

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
    if (args.length < 7) {
      println("Usage: SparkSpannerBenchmark <numRecords> <writeTable> <databaseId> <instanceId> <projectId> <resultsBucket> <buildSparkVersion> [mutationsPerTransaction]")
      sys.exit(1)
    }
    val numRecords = args(0).toLong
    val writeTable = args(1)
    val databaseId = args(2)
    val instanceId = args(3)
    val projectId = args(4)
    val resultsBucket = args(5)
    val buildSparkVersion = args(6)
    val mutationsPerTransaction = if (args.length > 7) Try(args(7).toInt).getOrElse(5000) else 5000

    val spark = SparkSession.builder().appName("SparkSpannerWriteBenchmark").getOrCreate()
    import spark.implicits._

    // UDF to generate random UUIDs for the primary key
    val generateUUID = udf(() => UUID.randomUUID().toString)
    
    println("Test: Writing data with new schema...")

    // Generate DataFrame with the new schema
    val dfWrite = spark
      .range(numRecords)
      .select(
        coalesce(generateUUID(), lit("0")).as("id"), // Only generate UUID for ID
        lit(payloadSample).as("json_payload"), // Use JSON from file
        lit("short_label_constant").as("short_label"),                     // Constant string
        lit(UUID.nameUUIDFromBytes("related_guid_constant".getBytes).toString).as("related_guid"), // Constant GUID
        lit("A").as("status_flag"),                                       // Constant single char
        current_timestamp().as("created_at"),                             // Current timestamp
        current_timestamp().as("updated_at")                              // Current timestamp
      )

    val targetSizeMb = 200
    // size per record is arbitrary guess
    val averageRowSizeBytes = 1085L
    val sizeInBytes = averageRowSizeBytes * numRecords
    val sizeMb = sizeInBytes / (1024 * 1024)
    val workerCount = 5;
    val coreCount = 4;
    val numPartitions = workerCount * coreCount * 2;
    println(s"Estimated job size: $sizeMb MB")
    println(f"Average row size: $averageRowSizeBytes%.2f bytes")
    println(s"Number of partitions: $numPartitions")

    val dfPartitioned = dfWrite.repartitionByRange(numPartitions.toInt, col("id")).sortWithinPartitions(col("id"))

    val bytesPerTransaction = 3*1024*1024
    val numWriteThreads = 4
    val maxPendingTransactions = 5
    val assumeIdempotentRows = true

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
      .option("numWriteThreads", numWriteThreads)
      .option("assumeIdempotentRows", assumeIdempotentRows.toString)
      .option("maxPendingTransactions", maxPendingTransactions.toString)
      .option("table", writeTable)
      .mode(SaveMode.Append)
      .save()
    val endTime = System.nanoTime()
    val durationSeconds = (endTime - startTime) / 1e9
    val throughput = sizeMb / durationSeconds

    println("Ending write")
    println(s"Write operation took: $durationSeconds seconds")
    println(f"Throughput: $throughput%.2f MB/s")

    val runId = UUID.randomUUID().toString.take(8)
    val runTimestamp = java.time.format.DateTimeFormatter.ISO_INSTANT.format(java.time.Instant.now())
    val sparkVersion = spark.version
    val connectorVersion = "0.1.0" // TODO: Get this from the build

    val json = s"""
    {
      "runId": "$runId",
      "runTimestamp": "$runTimestamp",
      "benchmarkName": "SparkSpannerWriteBenchmark",
      "clusterSparkVersion": "$sparkVersion",
      "connectorVersion": "$connectorVersion",
      "spannerConfig": {
        "projectId": "$projectId",
        "instanceId": "$instanceId",
        "databaseId": "$databaseId",
        "table": "$writeTable"
      },
      "benchmarkParameters": {
        "numRecords": $numRecords,
        "mutationsPerTransaction": $mutationsPerTransaction,
        "bytesPerTransaction": $bytesPerTransaction,
        "numWriteThreads": $numWriteThreads,
        "maxPendingTransactions": $maxPendingTransactions,
        "assumeIdempotentRows": $assumeIdempotentRows
      },
      "performanceMetrics": {
        "durationSeconds": $durationSeconds,
        "throughputMbPerSec": $throughput,
        "totalSizeMb": $sizeMb,
        "recordCount": $numRecords
      },
      "sparkConfig": {
        "numPartitions": $numPartitions,
        "workerCount": $workerCount,
        "coreCount": $coreCount
      }
    }
    """

    val resultsPath = s"gs://$resultsBucket/SparkSpannerWriteBenchmark/${runTimestamp}_$runId.json"
    println(s"Writing results to $resultsPath")

    val resultsURI = new URI(resultsPath)
    val fs = FileSystem.get(resultsURI, spark.sparkContext.hadoopConfiguration)
    val outputPath = new Path(resultsPath)
    val os = fs.create(outputPath, true) // true to overwrite
    val writer = new OutputStreamWriter(os, StandardCharsets.UTF_8)
    try {
      writer.write(json)
    } finally {
      writer.close()
      os.close()
    }

    println("Finished writing results.")

  }
}
