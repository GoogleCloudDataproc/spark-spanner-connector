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

import com.google.cloud.spanner.{DatabaseId, ResultSet, Spanner}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Utils {
  def buildSchemaSql(tableName: String): String = {
    s"""
       |SELECT
       |  t.column_name AS columnName,
       |  t.spanner_type AS spannerType,
       |  t.is_nullable AS isNullable
       |FROM
       |  information_schema.columns AS t
       |WHERE
       |  t.table_catalog = ''
       |  AND
       |  t.table_schema = ''
       |  AND
       |  t.table_name = '$tableName'
       |ORDER BY
       |  t.table_catalog,
       |  t.table_schema,
       |  t.table_name,
       |  t.ordinal_position
    """.stripMargin
  }

  def executeQuery(query: String)(implicit spanner: Spanner, dbId: DatabaseId): ResultSet = {
    val dbClient = spanner.getDatabaseClient(dbId)

    import com.google.cloud.spanner.Statement

    val stmt = Statement.of(query)
    dbClient.singleUse().executeQuery(stmt)
  }

  def printlnResultSet(rs: ResultSet): Unit = {
    try {
      while (rs.next()) {
        (0 until rs.getColumnCount).foreach { idx =>
          val value = rs.getString(idx)
          println(s"$idx. $value")
        }
      }
    } finally {
      rs.close()
    }
  }

  // FIXME Extension method on ResultSet?
  def toSparkSchema(rs: ResultSet): StructType = {
    // FIXME Get rid of this var
    var schema = new StructType()
    try {
      while (rs.next()) {
        val name = rs.getString("columnName")
        // FIXME Conversion between Spark's and Spanner's type systems
        val spannerType = rs.getString("spannerType") match {
          // FIXME Regex STRING(\d*)
          case dt if dt.startsWith("STRING") => "string"
          case dt => dt
        }
        val nullable = rs.getString("isNullable").equalsIgnoreCase("YES")
        schema = schema.add(name, spannerType, nullable)
      }
    } finally {
      rs.close()
    }
    schema
  }
}
