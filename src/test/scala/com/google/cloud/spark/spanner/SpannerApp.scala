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

import com.google.cloud.spanner.ResultSet

object SpannerApp extends App {

  val creds = sys.env.getOrElse("GOOGLE_APPLICATION_CREDENTIALS", {
    println("GOOGLE_APPLICATION_CREDENTIALS env var not defined. Exiting...")
    sys.exit(-1)
  })
  println(s"GOOGLE_APPLICATION_CREDENTIALS: $creds")

  import com.google.cloud.spanner.SpannerOptions
  val opts = SpannerOptions.newBuilder().build()
  implicit val spanner = opts.getService
  val instanceId = "dev-instance"
  val databaseId = "demo"

  val dbAdminClient = spanner.getDatabaseAdminClient
  import scala.collection.JavaConverters._
  dbAdminClient
    .listDatabases(instanceId)
    .iterateAll()
    .asScala
    .foreach(println)

  val db = dbAdminClient.getDatabase(instanceId, databaseId)
  println(s"Schema of Cloud Spanner database: $db")
  val ddls = db.getDdl.asScala
  ddls.foreach(println)

  import com.google.cloud.spanner.DatabaseId
  implicit val dbID = DatabaseId.of(opts.getProjectId, instanceId, databaseId)

  val table = "Account"

  println("")
  println(s"Get the schema of the table $table...")
  // FIXME Users don't like _root_, do they?
  val schemaRS = executeQuery(buildSchemaSql(table))
  printlnResultSet(schemaRS)

  println("")
  println("Print out the content of a table in a db to the stdout")

  val sql = s"SELECT * FROM $table"
  println(s"Executing SQL: $sql")

  val rs = executeQuery(sql)
  println("Records:")
  printlnResultSet(rs)

  // FIXME Use it as a spark connector
  // FIXME Work on push down predicates support

  // From javadoc of com.google.cloud.Service.Spanner:
  // "must be closed when it is no longer needed."
  spanner.close()

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
}
