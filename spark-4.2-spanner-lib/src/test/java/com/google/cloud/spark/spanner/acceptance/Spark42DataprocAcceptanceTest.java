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

import com.google.cloud.dataproc.v1.Job;
import com.google.cloud.dataproc.v1.JobStatus;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Ignore("Waiting for the Dataproc Spark 4.2 runtime")
@RunWith(JUnit4.class)
public final class Spark42DataprocAcceptanceTest extends DataprocAcceptanceTestBase {

  private static AcceptanceTestContext context;
  public static final String CONNECTOR_JAR_DIRECTORY = "../spark-4.2-spanner/target";

  public Spark42DataprocAcceptanceTest() {
    super(context);
  }

  @BeforeClass
  public static void setup() throws Exception {
    context =
        DataprocAcceptanceTestBase.setup(
            "3.0-debian13", CONNECTOR_JAR_DIRECTORY, "spark-4.2-spanner", ImmutableList.of());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    DataprocAcceptanceTestBase.tearDown(context);
  }

  @Test
  public void testJoin() throws Exception {
    String testName = "test-join";
    Job result =
        createAndRunPythonJob(
            testName,
            "read_test_join_pushdown.py",
            null,
            Arrays.asList(context.getResultsDirUri(testName), PROJECT_ID, INSTANCE_ID, DATABASE_ID),
            300);
    assertThat(result.getStatus().getState()).isEqualTo(JobStatus.State.DONE);
    String output = AcceptanceTestUtils.getCsv(context.getResultsDirUri(testName));
    assertThat(output.trim()).startsWith("PASS");
  }
}
