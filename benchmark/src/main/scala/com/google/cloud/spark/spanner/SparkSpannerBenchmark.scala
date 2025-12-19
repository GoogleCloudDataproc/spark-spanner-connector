package com.google.cloud.spark.spanner

import org.apache.spark.sql.functions.{coalesce, col, current_timestamp, lit, udf}
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.UUID
import scala.io.Source
import scala.util.Try

object SparkSpannerBenchmark {
  val payload_sample =
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
    // Expected arguments: numRecords, writeTable, [mutationsPerTransaction]
    if (args.length < 2) {
      println("Usage: DatabricksCorrectnessTest <numRecords> <write_table> [mutationsPerTransaction]")
      sys.exit(1)
    }
    val numRecords = args(0).toLong
    val writeTable = args(1)
    val mutationsPerTransaction = if (args.length > 2) Try(args(2).toInt).getOrElse(5000) else 5000

    val spark = SparkSession.builder().appName("DatabricksSpannerTests").getOrCreate()

    // UDF to generate random UUIDs for the primary key
    val generateUUID = udf(() => UUID.randomUUID().toString)
    
    val (projectId, instanceId, databaseId) = ("improvingvancouver", "mksyunz-spark-dev", "mini-test-gsql")

    try {
      println("Test: Writing data with new schema...")
      
      // Generate DataFrame with the new schema
      val dfWrite = spark
        .range(numRecords)
        .select(
          coalesce(generateUUID(), lit("0")).as("id"), // Only generate UUID for ID
          lit(payload_sample).as("json_payload"), // Use JSON from file
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
      //val numPartitions = (sizeMb + targetSizeMb - 1) / targetSizeMb
      val workerCount = 5;
      val coreCount = 4;
      val numPartitions = workerCount * coreCount * 2;
      println(s"Estimated job size: $sizeMb MB")
      println(f"Average row size: $averageRowSizeBytes%.2f bytes")
      println(s"Number of partitions: $numPartitions")

      val dfPartitioned = dfWrite.repartitionByRange(numPartitions.toInt, col("id")).sortWithinPartitions(col("id"))

      println(s"Beginning write to table '$writeTable' with mutationsPerTransaction: $mutationsPerTransaction")
      val startTime = System.nanoTime()
      dfPartitioned
        .write
        .format("cloud-spanner")
        .option("mutationsPerTransaction", mutationsPerTransaction)
        .option("bytesPerTransaction", (3*1024*1024).toString)
        .option("projectId", projectId)
        .option("instanceId", instanceId)
        .option("databaseId", databaseId)
        .option("numWriteThreads", 4)
        .option("assumeIdempotentRows", "true")
        .option("maxPendingTransactions", "5")
        .option("table", writeTable)
        .mode(SaveMode.Append)
        .save()
      val endTime = System.nanoTime()
      val durationSeconds = (endTime - startTime) / 1e9
      val throughput = sizeMb / durationSeconds

      println("Ending write")
      println(s"Write operation took: $durationSeconds seconds")
      println(f"Throughput: $throughput%.2f MB/s")
    } finally {
    }
  }
}
