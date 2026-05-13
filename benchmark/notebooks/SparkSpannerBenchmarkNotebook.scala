%scala
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{coalesce, col, current_timestamp, lit, udf, row_number}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.expressions.Window
import org.apache.spark.util.SizeEstimator
import java.io.OutputStreamWriter
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.time.Duration
import spark.implicits._
import play.api.libs.json.{JsValue, Json}

println("Running Spark Spanner Connector Benchmark...")

// 1. Get the single config widget
val configStr = dbutils.widgets.get("config")
val config: JsValue = Json.parse(configStr)

// 2. Extract values from the JSON blob
val projectId = (config \ "projectId").as[String]
val instanceId = (config \ "instanceId").as[String]
val databaseId = (config \ "databaseId").as[String]
val writeTable = (config \ "writeTable").as[String]
val sourceTable = (config \ "sourceTable").as[String]
val resultsBucket = (config \ "resultsBucket").as[String]
val buildSparkVersion = (config \ "buildSparkVersion").as[String]

// Helper for optional numeric values in the JSON
def getInt(key: String, default: Int): Int = (config \ key).asOpt[Int].getOrElse(default)
def getLong(key: String, default: Long): Long = (config \ key).asOpt[Long].getOrElse(default)
def getBool(key: String, default: Boolean): Boolean = (config \ key).asOpt[Boolean].getOrElse(default)

val numRecords = getLong("numRecords", 100000L)
val mutationsPerTransaction = getInt("mutationsPerTransaction", 5000)
val bytesPerTransaction = getLong("bytesPerTransaction", 3 * 1024 * 1024L)
val numWriteThreads = getInt("numWriteThreads", 4)
val maxPendingTransactions = getInt("maxPendingTransactions", 5)
val assumeIdempotentRows = getBool("assumeIdempotentRows", true)
val numPartitions = getInt("numPartitions", 40)

if (numRecords > Int.MaxValue) {
  dbutils.notebook.exit(s"ERROR: numRecords ($numRecords) exceeds the maximum value for an Integer (${Int.MaxValue}) and cannot be used with the 'limit' function.")
}

if (sourceTable.isEmpty) {
  dbutils.notebook.exit("ERROR: sourceTable widget cannot be empty.")
}


println(s"Reading from source table '$sourceTable'...")
val dfSource = spark.read.table(sourceTable)

val sourceTableCount = dfSource.count() // Get the total count of the source table

println(s"Source table '$sourceTable' has $sourceTableCount records.")

if (numRecords > sourceTableCount) {
  dbutils.notebook.exit(s"ERROR: Requested number of records ($numRecords) is greater than available records in source table ($sourceTableCount). Aborting benchmark.")
}

println(s"Selecting $numRecords records for the benchmark...")
val dfWrite = dfSource.limit(numRecords.toInt).cache()

val averageRowSizeBytes = SizeEstimator.estimate(dfWrite.first())
val sizeInBytes = averageRowSizeBytes * numRecords
val sizeMb = sizeInBytes / (1024.0 * 1024.0)
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
val endTime = System.nanoTime()
val durationSeconds = Duration.ofNanos(endTime - startTime).toMillis / 1000.0
val throughput = sizeMb / durationSeconds

println("Ending write")
println(s"Write operation took: $durationSeconds seconds")
println(f"Throughput: $throughput%.2f MB/s")

val runId = UUID.randomUUID().toString.take(8)
val runTimestamp = java.time.format.DateTimeFormatter.ISO_INSTANT.format(java.time.Instant.now())
val sparkVersion = spark.version
val connectorVersion = "0.0.1-SNAPSHOT"

val resultJson =
  s"""{
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
      "numPartitions": $numPartitions
    }
  }"""

// Expects path for Application Default Credentials (ADC) file
val gcpADCKeyFilePath = "/root/.config/gcloud/application_default_credentials.json"
println(s"Configuring GCS connector with Application Default Credentials from: $gcpADCKeyFilePath")

// Set Hadoop configuration for GCS connector using the ADC file
spark.sparkContext.hadoopConfiguration.set("google.cloud.auth.service.account.enable", "true")
spark.sparkContext.hadoopConfiguration.set("google.cloud.auth.service.account.json.keyfile", gcpADCKeyFilePath)

val resultsPath = s"gs://$resultsBucket/SparkSpannerWriteBenchmarkNotebook/${runTimestamp}_$runId.json"
println(s"Writing results to $resultsPath")

val resultsURI = new URI(resultsPath)
val fs = FileSystem.get(resultsURI, spark.sparkContext.hadoopConfiguration)
val outputPath = new Path(resultsPath)
val os = fs.create(outputPath, true)
val writer = new OutputStreamWriter(os, StandardCharsets.UTF_8)
try {
  writer.write(resultJson)
} finally {
  writer.close()
  os.close()
}

println("Finished writing results.")

dfWrite.unpersist()
dbutils.notebook.exit(resultsURI.toString())
