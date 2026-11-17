// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.cloud.spark.spanner.acceptance;

import static com.google.common.truth.Truth.assertThat;

import com.google.api.gax.longrunning.OperationSnapshot;
import java.util.Arrays;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * The acceptance test on Dataproc Serverless image 4.0 (Spark 4.2). The test has to run on a
 * project with requireOsLogin disabled, otherwise an org policy violation error will be thrown.
 */
// TODO: Enable once Dataproc Serverless image 4.0 is usable in CI.
@Ignore("Waiting for Dataproc Serverless image 4.0 support in CI")
@RunWith(JUnit4.class)
public final class Spark42DataprocServerlessImage40AcceptanceTest
    extends DataprocServerlessAcceptanceTestBase {

  public static final String CONNECTOR_JAR_DIRECTORY = "../spark-4.2-spanner/target";

  public Spark42DataprocServerlessImage40AcceptanceTest() {
    super("4.0");
  }

  @BeforeClass
  public static void setUp() throws Exception {
    setup(CONNECTOR_JAR_DIRECTORY, "spark-4.2-spanner");
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    teardown();
  }

  @Test
  public void testJoin() throws Exception {
    context = generateContext("join");
    OperationSnapshot operationSnapshot =
        createAndRunPythonBatch(
            context,
            testName,
            "read_test_join_pushdown.py",
            null,
            Arrays.asList(
                context.getResultsDirUri(testName), PROJECT_ID, INSTANCE_ID, DATABASE_ID));
    assertThat(operationSnapshot.isDone()).isTrue();
    assertThat(operationSnapshot.getErrorMessage()).isEmpty();
    String output = AcceptanceTestUtils.getCsv(context.getResultsDirUri(testName));
    assertThat(output.trim()).startsWith("PASS");
  }
}
