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
package com.google.cloud.spark.spanner

import com.google.cloud.spanner.Spanner
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType

case class SpannerRelation(spark: SparkSession, options: SpannerOptions)
  extends BaseRelation
    with PrunedFilteredScan
    with FilterConversion
    with InsertableRelation
    with Logging {

  override def sqlContext: SQLContext = spark.sqlContext

  override val schema: StructType = {
    import com.google.cloud.spanner.SpannerOptions
    val opts = SpannerOptions.newBuilder().build()
    implicit val spanner: Spanner = opts.getService
    import com.google.cloud.spanner.DatabaseId
    implicit val dbID: DatabaseId = DatabaseId.of(
      opts.getProjectId, options.instanceId, options.databaseId)
    val query = buildSchemaSql(options.table)
    try {
      val schemaRS = executeQuery(query)
      toSparkSchema(schemaRS)
    } finally {
      spanner.close()
    }
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    logDebug(s"requiredColumns: ${requiredColumns.mkString(", ")}")
    logDebug(s"filters: ${filters.mkString(", ")}")
    new SpannerRDD(spark.sparkContext, requiredColumns, filters, options)
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    val unhandled = filters.filter(f => toSql(f).isEmpty)
    logDebug(s"Unhandled filters: ${unhandled.mkString(", ")}")
    unhandled
  }

  override def toString: String = {
    s"Spanner(ID: ${options.instanceId}, ${options.databaseId}, ${options.table})"
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    data.write
      .mode(if (overwrite) SaveMode.Overwrite else SaveMode.Append)
      .format(SpannerRelationProvider.shortName)
      .options(options.options)
      .save
  }
}
