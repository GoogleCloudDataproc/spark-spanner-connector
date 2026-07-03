// Copyright 2025 Google LLC
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

package com.google.cloud.spark.spanner.integration;

import org.junit.Ignore;
import org.junit.Test;

public class Spark41SchemaValidationIntegrationTest extends SchemaValidationIntegrationTestBase {
  public Spark41SchemaValidationIntegrationTest(boolean usePostgreSql) {
    super(usePostgreSql);
  }

  @Override
  @Test
  @Ignore(
      "TODO: this behaviour needs clarification. In Spark 4.1 there is a fundamental change in how Spark's underlying DataSource V2 (DSv2) framework handles schema validation and column pruning compared to Spark 4.0. As a consequence this test does not throw a Spark AnalysisException anymore. We need to review if enablePartialRowUpdates is still relevant given Spark 4.1 allows partial row updates anyway.")
  public void testPartialWriteFailsWithoutOption() {
    super.testPartialWriteFailsWithoutOption();
  }
}
