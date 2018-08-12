package spanner.spark

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

abstract class BaseSpec extends FlatSpec
  with Matchers
  with BeforeAndAfterAll {

  override def beforeAll() {
    sys.env.get("GOOGLE_APPLICATION_CREDENTIALS").orElse {
      fail("GOOGLE_APPLICATION_CREDENTIALS env var not defined")
    }
  }

  def withSparkSession(testCode: SparkSession => Any): Unit = {
    val spark = SparkSession.builder.master("local[*]").getOrCreate
    try testCode(spark)
    finally spark.close
  }
}
