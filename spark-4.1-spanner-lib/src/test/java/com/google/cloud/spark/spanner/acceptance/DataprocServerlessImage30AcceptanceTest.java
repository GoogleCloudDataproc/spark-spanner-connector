// Copyright 2023 Google LLC
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
 * The acceptance test on the Dataproc Serverless. The test have to be running on the project with
 * requireOsLogin disabled, otherwise an org policy violation error will be thrown.
 */
@RunWith(JUnit4.class)
public final class DataprocServerlessImage30AcceptanceTest
    extends DataprocServerlessAcceptanceTestBase {

  public static final String CONNECTOR_JAR_DIRECTORY = "../spark-4.1-spanner/target";

  @BeforeClass
  public static void setUp() throws Exception {
    setup(CONNECTOR_JAR_DIRECTORY, "spark-4.1-spanner");
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    teardown();
  }

  @Ignore(
      "Skipping this test until Managed Service for Apache Spark 3.0 is shipped with Spark 4.1+.")
  @Test
  public void testJoin() throws Exception {
    // Provide a unique test name to identify the batch associated with this test.
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
    assertThat(output.trim()).isEqualTo("PASS");
  }

  public DataprocServerlessImage30AcceptanceTest() {
    super("3.0");
  }
}
