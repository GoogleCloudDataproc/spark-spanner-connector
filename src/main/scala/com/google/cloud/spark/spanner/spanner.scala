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
package com.google.cloud.spark

import java.sql.{Date, Timestamp}

import com.google.cloud
import com.google.cloud.spanner.{DatabaseId, Mutation, ResultSet, Spanner, SpannerException, Statement, TransactionContext}
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

package object spanner extends Logging {
  def buildSchemaSql(tableName: String): String = {
    s"""
       |SELECT
       |  t.column_name AS columnName,
       |  t.spanner_type AS spannerType,
       |  t.is_nullable = "YES" AS isNullable
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

  def isTableAvailable(instance: String, database: String, tableName: String): Boolean = {
    val schemaSql =
      """
        |SELECT
        |  t.table_name
        |FROM
        |  information_schema.tables AS t
        |WHERE
        |  t.table_catalog = ''
        |  AND
        |  t.table_schema = ''
        |  AND
        |  t.table_name = @tableName
      """.stripMargin
    val stmt = Statement
      .newBuilder(schemaSql)
      .bind("tableName").to(tableName)
      .build
    withSpanner { spanner =>
      import com.google.cloud.spanner.DatabaseId
      val db = DatabaseId.of(spanner.getOptions.getProjectId, instance, database)
      val rs = executeQuery(stmt)(spanner, db)
      rs.next()
    }.asInstanceOf[Boolean] // FIXME Get rid of asInstanceOf
  }

  def withSpanner(action: Spanner => Any): Any = {
    import com.google.cloud.spanner.SpannerOptions
    val opts = SpannerOptions.newBuilder().build()
    val spanner: Spanner = opts.getService
    try action(spanner)
    finally spanner.close()
  }

  def createTable(
    instance: String,
    database: String,
    tableName: String,
    schema: String,
    primaryKey: String): Unit = {

    val createTableSQL =
      s"""
         |CREATE TABLE $tableName (
         |	$schema
         |) PRIMARY KEY ($primaryKey)
       """.stripMargin

    withSpanner { spanner =>
      implicit val dbID: DatabaseId =
        DatabaseId.of(spanner.getOptions.getProjectId, instance, database)
      executeCreateTable(createTableSQL)(spanner, instance, database)
    }
  }

  @deprecated("Use executeQuery(stmt: Statement)")
  def executeQuery(query: String)(implicit spanner: Spanner, dbId: DatabaseId): ResultSet = {
    val dbClient = spanner.getDatabaseClient(dbId)

    import com.google.cloud.spanner.Statement

    logDebug(s"Executing query: $query")
    val stmt = Statement.of(query)
    dbClient.singleUse().executeQuery(stmt)
  }

  import com.google.cloud.spanner.Statement
  def executeQuery(stmt: Statement)(implicit spanner: Spanner, dbId: DatabaseId): ResultSet = {
    val dbClient = spanner.getDatabaseClient(dbId)
    logDebug(s"Executing query: $stmt")
    dbClient.singleUse().executeQuery(stmt)
  }

  def executeQuery(
    stmt: Statement,
    tx: TransactionContext): ResultSet = {
    logDebug(s"Executing query: $stmt (in $tx)")
    tx.executeQuery(stmt)
  }

  @throws(classOf[SpannerException])
  def executeCreateTable(stmt: String)
    (implicit spanner: Spanner, instance: String, database: String): Unit = {
    val dbClient = spanner.getDatabaseAdminClient
    logDebug(s"Executing query: $stmt")
    import scala.collection.JavaConverters._
    val op = dbClient.updateDatabaseDdl(instance, database, Iterable(stmt).asJava, null)
    op.get()
  }

  def dropTables(spanner: Spanner, instance: String, database: String, names: String*): Unit = {
    logDebug(s"Dropping tables: ${names.mkString(", ")}")
    val dbClient = spanner.getDatabaseAdminClient
    val drops = names.map(t => s"DROP TABLE $t")
    import scala.collection.JavaConverters._
    val op = dbClient.updateDatabaseDdl(instance, database, drops.asJava, null)
    op.get()
  }

  // FIXME Extension method on ResultSet?
  /**
    * @see <a href="https://cloud.google.com/spanner/docs/data-types">Data Types</a>
    * @return Spark-compatible schema
    */
  def toSparkSchema(rs: ResultSet): StructType = {
    var schema = new StructType()
    try {
      while (rs.next()) {
        val name = rs.getString("columnName")
        val spannerType = rs.getString("spannerType")
        val sparkType = toCatalystType(spannerType)
        val nullable: Boolean = rs.getBoolean("isNullable")
        schema = schema.add(name, sparkType, nullable, comment = spannerType)
      }
    } finally {
      rs.close()
    }
    schema
  }

  import org.apache.spark.sql.types._
  def toCatalystType(spannerType: String): DataType = {
    val STRING = """STRING\(\s*(\S+)\s*\)""".r
    val BYTES = """BYTES\(\s*(\S+)\s*\)""".r
    val ARRAY = """ARRAY<(\S+)>""".r
    spannerType match {
      // scalar types
      case STRING(_) => StringType
      case "BOOL" => BooleanType
      case "INT64" => LongType
      case "FLOAT64" => DoubleType
      case BYTES(_) => ByteType
      case "DATE" => DateType
      case "TIMESTAMP" => TimestampType
      // array type
      // array of arrays is not allowed
      case ARRAY(t) if !(t startsWith "ARRAY") => ArrayType(toCatalystType(t))
    }
  }

  /**
    * @see <a href="https://cloud.google.com/spanner/docs/data-definition-language">Data Definition Language</a>
    * @return Spanner DDL for write schema
    */
  def toSpannerDDL(schema: StructType): String = {
    // NOTE Ending comma allowed
    schema.map(toSpannerDDL).mkString(",")
  }

  def toSpannerDDL(f: StructField): String = {
    val fieldName = f.name
    val fieldType = toSpannerType(f.dataType)
    val notNull = if (f.nullable) "" else "NOT NULL"
    s"$fieldName $fieldType $notNull"
  }

  /**
    * @see <a href="https://cloud.google.com/spanner/docs/data-definition-language">Data Definition Language</a>
    * @return Spanner type
    */
  def toSpannerType(catalystType: DataType): String = {
    catalystType match {
      case BooleanType => "BOOL"
      case BinaryType => "BYTES(1)"
      case ByteType => "BYTES(MAX)"
      case _: LongType | _: ShortType | _: IntegerType => "INT64"
      case _: DoubleType | _: FloatType => "FLOAT64"
      case StringType => "STRING(MAX)"
      case DateType => "DATE"
      case TimestampType => "TIMESTAMP"
      case ArrayType(elementType, _) if !elementType.isInstanceOf[ArrayType] =>
        s"ARRAY<${toSpannerType(elementType)}>"
      case a @ ArrayType(elementType, _) if elementType.isInstanceOf[ArrayType] =>
        throw new IllegalArgumentException(
          s"""
             |${a.sql} is not supported in Cloud Spanner.
             |Please consult https://cloud.google.com/spanner/docs/data-definition-language#arrays
             |regarding the syntax for using the ARRAY type in DDL.
             |You could use explode standard function to flatten the array.
           """.stripMargin)
      case t =>
        // MapType
        // StructType
        // ArrayType(ArrayType(...))
        // CalendarIntervalType (handled by DataSource.planForWriting)
        throw new IllegalArgumentException(s"${t.sql} is not supported")
    }
  }

  def savePartition(
    instance: String,
    database: String,
    tableName: String,
    schema: StructType,
    primaryKey: String,
    it: Iterator[Row]): Unit = {
    val pid = TaskContext.get.partitionId()
    logDebug(s"Saving partition $pid")
    val ms = scala.collection.mutable.ArrayBuffer.empty[Mutation]
    it.foreach { row =>
      val wb = Mutation.newInsertBuilder(tableName)
      schema.foldLeft(wb) { case (mb, field) =>
        val name = field.name
        field.dataType match {
          case BooleanType =>
            mb.set(name).to(row.getAs[Boolean](name))
          case BinaryType =>
            mb.set(name).to(row.getAs[Byte](name))
          case ByteType =>
            mb.set(name).to(row.getAs[Byte](name))
          case _: LongType | _: ShortType | _: IntegerType =>
            mb.set(name).to(row.getAs[Long](name))
          case DoubleType =>
            mb.set(name).to(row.getAs[Double](name))
          case FloatType =>
            mb.set(name).to(row.getAs[Float](name))
          case StringType =>
            mb.set(name).to(row.getAs[String](name))
          case DateType =>
            val d = row.getAs[Date](name).toLocalDate
            val value = cloud.Date.fromYearMonthDay(d.getYear, d.getMonthValue, d.getDayOfMonth)
            mb.set(name).to(value)
          case TimestampType =>
            val ts = row.getAs[Timestamp](name)
            val value = cloud.Timestamp.of(ts)
            mb.set(name).to(value)
          case _ => ???
        }
      }
      ms += wb.build()
    }
    withSpanner { spanner =>
      import com.google.cloud.spanner.DatabaseId
      val db = DatabaseId.of(spanner.getOptions.getProjectId, instance, database)
      val dbClient = spanner.getDatabaseClient(db)
      import scala.collection.JavaConverters._
      dbClient.write(ms.asJava)
    }
  }
}
