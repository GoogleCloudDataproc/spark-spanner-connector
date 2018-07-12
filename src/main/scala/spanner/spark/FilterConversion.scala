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
