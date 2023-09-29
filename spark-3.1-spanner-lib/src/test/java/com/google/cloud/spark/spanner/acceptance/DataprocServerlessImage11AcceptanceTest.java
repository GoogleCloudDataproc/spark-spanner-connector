package com.google.cloud.spark.spanner.acceptance;

import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * The acceptance test on the Dataproc Serverless. The test have to be running on the project with
 * requireOsLogin disabled, otherwise an org policy violation error will be thrown.
 */
@RunWith(JUnit4.class)
public final class DataprocServerlessImage11AcceptanceTest
    extends DataprocServerlessAcceptanceTestBase {

  private static AcceptanceTestContext context;

  public DataprocServerlessImage11AcceptanceTest() {
    super("spark-3.1-spanner", "1.1");
  }
}
