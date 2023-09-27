package com.google.cloud.spark.spanner.acceptance;

import com.google.common.collect.ImmutableList;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class DataprocImage20AcceptanceTest extends DataprocAcceptanceTestBase {

  private static AcceptanceTestContext context;

  public DataprocImage20AcceptanceTest() {
    super(context);
  }

  @BeforeClass
  public static void setup() throws Exception {
    context =
        DataprocAcceptanceTestBase.setup("2.0-debian10", "spark-3.1-spanner", ImmutableList.of());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    DataprocAcceptanceTestBase.tearDown(context);
  }

  @Test
  public void testVersion() {}
}
