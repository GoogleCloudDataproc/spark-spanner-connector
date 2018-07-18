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

import com.google.cloud.spanner.Spanner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import spanner.spark.Utils.{buildSchemaSql, executeQuery}

// FIXME with InsertableRelation (as non-FileFormats, e.g. Kafka and JDBC)
case class SpannerRelation(spark: SparkSession, options: SpannerOptions)
  extends BaseRelation
  with PrunedFilteredScan
  with FilterConversion {

  override def sqlContext: SQLContext = spark.sqlContext

  override val schema: StructType = {
    import com.google.cloud.spanner.SpannerOptions
    val opts = SpannerOptions.newBuilder().build()
    implicit val spanner: Spanner = opts.getService
    try {
      import com.google.cloud.spanner.DatabaseId
      implicit val dbID: DatabaseId = DatabaseId.of(
        opts.getProjectId, options.instanceId, options.databaseId)

      val schemaRS = executeQuery(buildSchemaSql(options.table))
      Utils.toSparkSchema(schemaRS)
    } finally {
      spanner.close()
    }
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    println(s"buildScan: requiredColumns = ${requiredColumns.toSeq}")
    println(s"buildScan: filters = ${filters.toSeq}")
    new SpannerRDD(spark.sparkContext, requiredColumns, filters, options)
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    filters.filter(f => toSql(f).isEmpty)
  }

  override def toString: String = {
    s"Spanner(ID: ${options.instanceId}, ${options.databaseId}, ${options.table})"
  }
}
