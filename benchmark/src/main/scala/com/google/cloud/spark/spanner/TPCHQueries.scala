package com.google.cloud.spark.spanner

import scala.io.Source

object TPCHQueries {
  def getQuery(n: Int): String = {
    val resourcePath = s"sql/tpch/query-$n.sql" // Note: fromResource usually doesn't need leading /

    val stream = getClass.getClassLoader.getResourceAsStream(resourcePath)
    if (stream == null) {
      throw new IllegalArgumentException(s"Query $n not found at $resourcePath")
    }

    val source = Source.fromInputStream(stream)(StandardCharsets.UTF_8.name())
    try {
      source.mkString
    } finally {
      source.close()
    }
  }
}
