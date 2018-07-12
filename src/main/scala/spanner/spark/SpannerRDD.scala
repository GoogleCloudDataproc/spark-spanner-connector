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

import com.google.cloud.spanner.{ResultSet, Spanner}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources._
import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}

class SpannerRDD(
    sc: SparkContext,
    columns: Array[String],
    filters: Array[Filter],
    options: SpannerOptions)
  extends RDD[Row](sc, Nil) // FIXME Use InternalRow (not Row)
  with FilterConversion {
  // FIXME Number of partitions to leverage Spanner distribution
  // Number of partitions is the number of calls to compute (one per partition)
  // Can reads be distributed in Cloud Spanner?
  override protected def getPartitions: Array[Partition] = Array(SpannerPartition(0))

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {

    var closed = false
    var rs: ResultSet = null
    var spanner: Spanner = null

    def close(): Unit = {
      if (closed) return
      try {
        if (null != rs) {
          rs.close()
        }
      } catch {
        case e: Exception => logWarning("Exception closing Spanner ResultSet", e)
      }
      if (spanner != null) {
        spanner.close()
      }
      closed = true
    }

    context.addTaskCompletionListener { _ => close() }

    val cols = if (columns.isEmpty) {
      "*"
    } else {
      columns.mkString(",")
    }
    val whereClause = filters2WhereClause(filters)
    val sql = s"SELECT $cols FROM ${options.table} $whereClause"

    println(s"[SpannerRDD.compute] sql: $sql")

    import com.google.cloud.spanner.SpannerOptions
    val opts = SpannerOptions.newBuilder().build()
    spanner = opts.getService
    import com.google.cloud.spanner.DatabaseId
    val dbID = DatabaseId.of(opts.getProjectId, options.instanceId, options.databaseId)
    rs = Utils.executeQuery(sql)(spanner, dbID)

    val rowsIterator = new Iterator[Row]() {
      override def hasNext: Boolean = rs.next()

      override def next(): Row = {
        val values = columns.foldLeft(Seq.empty[Any]) { case (vs, colName) =>
          // FIXME Use the correct type (not String exclusively)
          // FIXME https://github.com/GoogleCloudPlatform/spanner-spark-connector/issues/2
          val value = rs.getString(colName)
          value +: vs
        }.reverse
        Row.fromSeq(values)
      }
    }

    new InterruptibleIterator(context, rowsIterator)
  }
}

case class SpannerPartition(override val index: Int) extends Partition