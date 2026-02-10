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

// Declare widgets to define parameters for the notebook
dbutils.widgets.text("projectId", "", "GCP Project ID")
dbutils.widgets.text("instanceId", "", "Spanner Instance ID")
dbutils.widgets.text("databaseId", "", "Spanner Database ID")
dbutils.widgets.text("writeTable", "", "Spanner Table Name")
dbutils.widgets.text("numRecords", "100000", "Number of records to write")
dbutils.widgets.text("mutationsPerTransaction", "5000", "Mutations per transaction")
dbutils.widgets.text("bytesPerTransaction", (3 * 1024 * 1024L).toString, "Bytes per transaction")
dbutils.widgets.text("numWriteThreads", "4", "Number of write threads")
dbutils.widgets.text("maxPendingTransactions", "5", "Maximum pending transactions")
dbutils.widgets.text("assumeIdempotentRows", "true", "Assume idempotent rows")
dbutils.widgets.text("resultsBucket", "", "GCS Bucket for results")
dbutils.widgets.text("buildSparkVersion", "3.3", "Spark version used for the connector")
dbutils.widgets.text("numPartitions", "40", "Number of partitions for the DataFrame")
dbutils.widgets.text("sourceTable", "", "The name of the source Delta table to read from")

println("Running Spark Spanner Connector Benchmark...")

val projectId = dbutils.widgets.get("projectId")
val instanceId = dbutils.widgets.get("instanceId")
val databaseId = dbutils.widgets.get("databaseId")
val writeTable = dbutils.widgets.get("writeTable")
val sourceTable = dbutils.widgets.get("sourceTable")
val numRecords = dbutils.widgets.get("numRecords").toLong

// Use get for widgets, then fallback to default if empty
def getOrDefault(name: String, default: String): String = {
  val v = dbutils.widgets.get(name)
  if (v == null || v.isEmpty) default else v
}

val mutationsPerTransaction = getOrDefault("mutationsPerTransaction", "5000").toInt
val bytesPerTransaction = getOrDefault("bytesPerTransaction", (3 * 1024 * 1024L).toString).toLong
val numWriteThreads = getOrDefault("numWriteThreads", "4").toInt
val maxPendingTransactions = getOrDefault("maxPendingTransactions", "5").toInt
val assumeIdempotentRows = getOrDefault("assumeIdempotentRows", "true").toBoolean
val resultsBucket = dbutils.widgets.get("resultsBucket")
val buildSparkVersion = dbutils.widgets.get("buildSparkVersion")
val generateUUID = udf(() => UUID.randomUUID().toString)

val numPartitions = getOrDefault("numPartitions", "40").toInt

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
val endTime = System.nanoTime()
val durationSeconds = Duration.ofNanos(endTime - startTime).toMillis
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
dbutils.notebook.exit("SUCCESS")
