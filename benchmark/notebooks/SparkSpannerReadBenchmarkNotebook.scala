// Databricks notebook source
import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.functions.{col, round, trim}
import org.apache.spark.sql.types._
import play.api.libs.json._
import play.api.libs.json.Reads._
import play.api.libs.json.{Json => PlayJson}
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.OutputStreamWriter
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.UUID
import com.google.cloud.spark.spanner.TPCHQueries
import java.io.File

// 1. Capture the configuration passed from BenchmarkingTasks.scala
// Databricks passes arguments as widgets
println("Step 1 - capture configuration")

val configStr = dbutils.widgets.get("config")
val config: JsValue = PlayJson.parse(configStr)

val projectId = (config \ "projectId").as[String]
val instanceId = (config \ "instanceId").as[String]
val databaseId = (config \ "databaseId").as[String]
val resultsBucket = (config \ "resultsBucket").as[String]
val buildSparkVersion = (config \ "buildSparkVersion").as[String]
val queryNumber = (config \ "tpcQueryNumber").as[Int]
val tables = (config \ "tpchTables").as[Seq[String]]
val provider = s"com.google.cloud.spark.spanner.Spark${buildSparkVersion.replace(".", "")}SpannerTableProvider"

println(s"projectId: $projectId")
println(s"instanceId: $instanceId")
println(s"databaseId: $databaseId")
println(s"resultsBucket: $resultsBucket")
println(s"buildSparkVersion: $buildSparkVersion")
println(s"queryNumber: $queryNumber")

// 2. Register TPC-H tables as Temp Views
println("Step 2 - Register TPC-H tables as Temp Views")

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
println("Step 2 - End")


// 3. Execution Logic
println("Step 3 - Execution Logic")

val sqlQuery = com.google.cloud.spark.spanner.TPCHQueries.getQuery(queryNumber)
val actualDf = spark.sql(sqlQuery)

actualDf.cache()

println(s"Starting TPC-H Query $queryNumber on Databricks...")
val startTime = System.nanoTime()

val resultCount = actualDf.count() // Using count() to trigger execution in Databricks

val endTime = System.nanoTime()
val durationSeconds = (endTime - startTime) / 1e9

println(s"Query $queryNumber finished in $durationSeconds seconds.")

// 4. Validation and Persistence
println("Step 4 - Validation and Persistence")

// Configure GCP access to read expected query responses and save results
val gcpADCKeyFilePath = "/root/.config/gcloud/application_default_credentials.json"
spark.sparkContext.hadoopConfiguration.set("fs.gs.auth.service.account.enable", "true")
spark.sparkContext.hadoopConfiguration.set("fs.gs.auth.service.account.json.keyfile", gcpADCKeyFilePath)

val isValid = validateQueryOutput(actualDf, queryNumber, resultsBucket)

println(s"validateQueryOutput is $isValid")

saveResults(resultsBucket, queryNumber, durationSeconds, resultCount, isValid)

// Exit notebook with a result string
println("Step 5 - Exit notebook")
dbutils.notebook.exit(s"Success: Q$queryNumber")

def validateFilePath(path: String): Boolean = {
  val file = new File(path)

  if (!file.exists()) {
    println(s"❌ ERROR: File not found at: $path")
    false
  } else if (!file.isFile) {
    println(s"❌ ERROR: Path exists but is a directory, not a file: $path")
    false
  } else if (!file.canRead) {
    println(s"❌ ERROR: File exists but is not readable: $path")
    false
  } else {
    println(s"✅ SUCCESS: Verified file exists and is readable (${file.length()} bytes): $path")
    true
  }
}

def normalize(df: DataFrame): DataFrame = {
  val normalizedCols = df.schema.fields.map { field =>
    field.dataType match {
      case _: DoubleType | _: FloatType | _: DecimalType =>
        round(col(field.name), 2).cast(DecimalType(38, 2)).as(field.name)
      case StringType =>
        trim(col(field.name)).as(field.name)
      case _ =>
        col(field.name)
    }
  }
  df.select(normalizedCols: _*)
}

def validateQueryOutput(actualDf: DataFrame, queryNum: Int, referenceBucket: String): Boolean = {
  val referencePath = s"gs://$referenceBucket/answers/q$queryNum.out"

  val expectedDf = spark.read
    .option("sep", "|")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("ignoreLeadingWhiteSpace", "true")
    .option("ignoreTrailingWhiteSpace", "true")
    .csv(referencePath)

  val actualNormalized = normalize(actualDf)
  val expectedNormalized = normalize(expectedDf)

  val actualSorted = actualNormalized.sort(actualNormalized.columns.map(col): _*)
  val expectedSorted = expectedNormalized.sort(actualNormalized.columns.map(col): _*)

  val actualExtra = actualSorted.except(expectedSorted)
  val expectedExtra = expectedSorted.except(actualSorted)

  val totalMismatches = actualExtra.count() + expectedExtra.count()

  if (totalMismatches > 0) {
    println(s"Query $queryNum: FAILED ($totalMismatches mismatches)")
    actualExtra.show(5)
    false
  } else {
    println(s"Query $queryNum: PASSED")
    true
  }
}

def saveResults(bucket: String, qNum: Int, duration: Double, count: Long, isValid: Boolean): Unit = {
  println("saveResults start")
  val runId = UUID.randomUUID().toString.take(8)
  val runTimestamp = java.time.format.DateTimeFormatter.ISO_INSTANT.format(java.time.Instant.now())
  val resultJson = PlayJson.obj(
    "benchmarkName" -> "TPCH-Read-Databricks",
    "queryNumber" -> qNum,
    "durationSeconds" -> duration,
    "rowCount" -> count,
    "timestamp" -> runTimestamp,
    "isValid" -> isValid
  )

  val path = s"gs://$bucket/TPCH/Q${qNum}_${runId}.json"
  println(s"Writing results to $path")

  // Expects path for Application Default Credentials (ADC) file
  val fs = FileSystem.get(new URI(path), spark.sparkContext.hadoopConfiguration)
  val writer = new OutputStreamWriter(fs.create(new Path(path), true), StandardCharsets.UTF_8)
  writer.write(PlayJson.prettyPrint(resultJson))
  writer.close()
  println("saveResults end")
}
