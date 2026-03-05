// Databricks notebook source
// MAGIC %scala
import org.apache.spark.sql.functions.{coalesce, col, current_timestamp, lit, udf}
import java.util.UUID

// This notebook generates source data for the Spark Spanner benchmark
// and saves it as a Delta table.

// 1. Declare widgets
dbutils.widgets.text("sourceTable", "", "The name of the source Delta table to create")
dbutils.widgets.text("numRecords", "100000", "Number of records to generate")

// 2. Get parameters
val sourceTable = dbutils.widgets.get("sourceTable")
val numRecords = dbutils.widgets.get("numRecords").toLong

if (sourceTable.isEmpty) {
  dbutils.notebook.exit("ERROR: sourceTable widget cannot be empty.")
}

// 3. Define payload and schema
private val payloadSample =
  """
{
  "data": [
    {"id": 1, "value": "a_long_string_to_make_the_file_bigger_01"},
    {"id": 2, "value": "a_long_string_to_make_the_file_bigger_02"},
    {"id": 3, "value": "a_long_string_to_make_the_file_bigger_03"},
    {"id": 4, "value": "a_long_string_to_make_the_file_bigger_04"},
    {"id": 5, "value": "a_long_string_to_make_the_file_bigger_05"},
    {"id": 6, "value": "a_long_string_to_make_the_file_bigger_06"},
    {"id": 7, "value": "a_long_string_to_make_the_file_bigger_07"},
    {"id": 8, "value": "a_long_string_to_make_the_file_bigger_08"},
    {"id": 9, "value": "a_long_string_to_make_the_file_bigger_09"},
    {"id": 10, "value": "a_long_string_to_make_the_file_bigger_10"},
    {"id": 11, "value": "a_long_string_to_make_the_file_bigger_11"},
    {"id": 12, "value": "a_long_string_to_make_the_file_bigger_12"},
    {"id": 13, "value": "a_long_string_to_make_the_file_bigger_13"},
    {"id": 14, "value": "a_long_string_to_make_the_file_bigger_14"},
    {"id": 15, "value": "a_long_string_to_make_the_file_bigger_15"},
    {"id": 16, "value": "a_long_string_to_make_the_file_bigger_16"}
  ]
}
""".stripMargin

import spark.implicits._
val generateUUID = udf(() => UUID.randomUUID().toString)

println(s"Generating $numRecords records for source table '$sourceTable'...")

val df = spark
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

// 4. Write to Delta table
df.write.format("delta").mode("overwrite").saveAsTable(sourceTable)

println(s"Successfully created source Delta table '$sourceTable' with $numRecords records.")

dbutils.notebook.exit("SUCCESS")
