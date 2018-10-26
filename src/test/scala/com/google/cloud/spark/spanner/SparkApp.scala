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

import org.apache.spark.sql.SparkSession

object SparkApp extends App {

  val spark = SparkSession.builder.master("local[*]").getOrCreate()
  println(s"Running Spark ${spark.version}")

  val creds = sys.env.getOrElse("GOOGLE_APPLICATION_CREDENTIALS", "(undefined)")
  println(s"GOOGLE_APPLICATION_CREDENTIALS: $creds")

  val opts = Map(
    "instanceId" -> "dev-instance",
    "databaseId" -> "demo"
  )
  val table = "Account"
  // Until the format is ready, try-finally is to keep sbt shell clean
  // spark.close shuts down all the underlying Spark services
  // and they do not pollute the output
  try {
    val accounts = spark
      .read
      .format("cloud-spanner")
      .options(opts)
      .load(table)

    accounts.printSchema

    // FIXME Another candidate for type-related test
    println("Dump schema types (Spanner types in round brackets):")
    accounts.schema.fields
      .map(f => (f.name, f.dataType.typeName, f.getComment.get))
      .foreach { case (name, sparkType, spannerType) =>
        println(s" |-- $name: $sparkType (spanner: $spannerType)")
      }
    println("")
    accounts.show(truncate = false)

    import spark.implicits._
    // FIXME The following are the candidates for tests
    // FIXME Tests for all filters
    {
      val q = accounts.where($"name" startsWith "A")
      q.explain()
      val namesCount = q.count
      println(s"""Number of names matching (startsWith "A"): $namesCount""")
    }
    {
      val q = accounts.where($"name" isin ("just", "for", "testing"))
      q.explain()
      val namesCount = q.count
      println(s"Number of names matching isin: $namesCount")
    }

  } finally {
    spark.close()
  }
}
