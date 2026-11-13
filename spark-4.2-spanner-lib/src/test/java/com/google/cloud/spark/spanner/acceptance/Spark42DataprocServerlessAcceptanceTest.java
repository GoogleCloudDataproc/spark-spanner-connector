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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Ignore("Waiting for the serverless Spark 4.2 runtime")
@RunWith(JUnit4.class)
public final class Spark42DataprocServerlessAcceptanceTest
    extends DataprocServerlessAcceptanceTestBase {

  public static final String CONNECTOR_JAR_DIRECTORY = "../spark-4.2-spanner/target";

  @BeforeClass
  public static void setUp() throws Exception {
    setup(CONNECTOR_JAR_DIRECTORY, "spark-4.2-spanner");
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    teardown();
  }

  public Spark42DataprocServerlessAcceptanceTest() {
    super("3.0");
  }
}
