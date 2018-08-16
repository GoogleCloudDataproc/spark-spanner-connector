/*
 * Copyright 2018 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package spanner.spark

import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources._
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

class SpannerRelationProvider
  extends DataSourceRegister
    with RelationProvider
    with CreatableRelationProvider
    with Logging {

  override def shortName(): String = "cloud-spanner"

  override def createRelation(
      sqlContext: SQLContext,
      params: Map[String, String]): BaseRelation = {
    val options = SpannerOptions(params)
    logDebug(s"Creating SpannerRelation with options: $options")
    SpannerRelation(sqlContext.sparkSession, options)
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      params: Map[String, String],
      data: DataFrame): BaseRelation = {
    val options = SpannerOptions(params)
    val table = options.table
    val instance = options.instanceId
    val database = options.databaseId

    val writeSchema = options.writeSchema.getOrElse(toSpannerDDL(data.schema))
    val primaryKey = options.primaryKey

    logDebug(
      s"""
         |Saving DataFrame to Spanner table
         |  mode:     $mode
         |  table:    $table
         |  instance: $instance
         |  database: $database
         |  writeSchema: $writeSchema
         |  primaryKey:  $primaryKey
       """.stripMargin)

    if (isTableAvailable(instance, database, table)) {
      logDebug(s"Table $table exists")
      import SaveMode._
      mode match {
        case Overwrite =>
          withSpanner { spanner =>
            dropTables(spanner, instance, database, table)
          }
        case ErrorIfExists =>
          throw new IllegalStateException(
            s"Table '$table' already exists and SaveMode is ErrorIfExists.")
        case Ignore => // it's OK
        case Append => // it's also OK
      }
    } else {
      logDebug(s"Table $table does not exist and will be created")
    }
    createTable(instance, database, table, writeSchema, primaryKey)
    logDebug(s"Inserting data")
    val schema = data.schema
    data.rdd.foreachPartition { it =>
      savePartition(instance, database, table, schema, primaryKey, it)
    }
    logDebug(s"Creating SpannerRelation to represent the table: $table")
    createRelation(sqlContext, params)
  }
}
