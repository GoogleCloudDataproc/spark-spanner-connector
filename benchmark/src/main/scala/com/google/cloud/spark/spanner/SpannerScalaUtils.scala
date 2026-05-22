package com.google.cloud.spark.spanner

object SpannerScalaUtils {
  /**
   * Generates the provider class name based on the Spark version.
   * e.g., "3.3" -> "com.google.cloud.spark.spanner.Spark33SpannerTableProvider"
   */
  def getProviderClassName(sparkVersion: String): String = {
    val versionSuffix = sparkVersion.replace(".", "")
    s"com.google.cloud.spark.spanner.Spark${versionSuffix}SpannerTableProvider"
  }
}
