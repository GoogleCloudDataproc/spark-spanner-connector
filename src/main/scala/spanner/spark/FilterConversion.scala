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

import org.apache.spark.sql.execution.datasources.jdbc.JDBCRDD
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.sources._

trait FilterConversion {

  object SpannerDialect extends JdbcDialect {
    override def canHandle(url : String): Boolean = false
  }

  def toSql(f: Filter): Option[String] = {
    JDBCRDD.compileFilter(f, SpannerDialect)
  }

  def filters2WhereClause(filters: Array[Filter]): String = {
    println(s"filters2WhereClause: ${filters.toSeq}")
    val part = filters.flatMap(toSql).mkString(" AND ")
    if (part.isEmpty) "" else s"WHERE $part"
  }
}
