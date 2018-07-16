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

import java.sql.Date
import java.time.LocalDate

import com.google.cloud.spanner.{ResultSet, Spanner, Type}
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
          // FIXME Opposite of Utils.toSparkSchema
          import com.google.cloud.spanner.Type.Code._
          val value = rs.getColumnType(colName).getCode match {
            case BOOL => valueOrNull(colName, rs.getBoolean(colName))
            case INT64 => valueOrNull(colName, rs.getLong(colName))
            case FLOAT64 => valueOrNull(colName, rs.getDouble(colName))
            case BYTES => valueOrNull(colName, rs.getBytes(colName))
            case DATE => valueOrNull(colName, {
              val d = rs.getDate(colName)
              Date.valueOf(LocalDate.of(d.getYear, d.getMonth, d.getDayOfMonth))
            })
            case STRING => valueOrNull(colName, rs.getString(colName))
            case TIMESTAMP => valueOrNull(colName, rs.getTimestamp(colName).toSqlTimestamp)
            case ARRAY =>
              import com.google.cloud.spanner.Type._
              val tpe = rs.getColumnType(colName)
              tpe match {
                case t if t == array(bool()) => valueOrNull(colName, rs.getBooleanList(colName))
                case t if t == array(int64()) => valueOrNull(colName, rs.getLongList(colName))
                case t if t == array(float64()) => valueOrNull(colName, rs.getDoubleList(colName))
                case t if t == array(string()) => valueOrNull(colName, rs.getStringList(colName))
                case t if t == array(bytes()) => valueOrNull(colName, rs.getBooleanList(colName))
                case t if t == array(timestamp()) => valueOrNull(colName, rs.getTimestampList(colName))
                case t if t == array(date()) => valueOrNull(colName, rs.getDateList(colName))
              }
            case STRUCT => throw new IllegalArgumentException(
              """
                |STRUCT is not a valid column type
                |See https://cloud.google.com/spanner/docs/data-types#struct-type
              """.stripMargin)
          }
          value +: vs
        }.reverse
        Row.fromSeq(values)
      }

      private def valueOrNull(colName: String, value: => Any): Any = {
        if (rs.isNull(colName)) null else value
      }
    }

    new InterruptibleIterator(context, rowsIterator)
  }
}

case class SpannerPartition(override val index: Int) extends Partition