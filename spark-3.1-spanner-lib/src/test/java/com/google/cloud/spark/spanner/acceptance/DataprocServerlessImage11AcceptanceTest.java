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
  public static final String CONNECTOR_JAR_DIRECTORY = "../spark-3.1-spanner/target";

  public DataprocServerlessImage11AcceptanceTest() {
    super(CONNECTOR_JAR_DIRECTORY, "spark-3.1-spanner", "1.1");
  }
}
