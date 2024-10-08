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

public class AcceptanceTestContext {
  final String testId;
  final String clusterId;
  final String connectorJarUri;
  final String testBaseGcsDir;
  final String spannerDataset;
  final String spannerTable;

  public AcceptanceTestContext(
      String testId, String clusterId, String testBaseGcsDir, String connectorJarUri) {
    this.testId = testId;
    this.clusterId = clusterId;
    this.testBaseGcsDir = testBaseGcsDir;
    this.connectorJarUri = connectorJarUri;
    this.spannerDataset = "spanner_acceptance_test_dataset_" + testId.replace("-", "_");
    this.spannerTable = "spanner_acceptance_test_table_" + testId.replace("-", "_");
  }

  public String getScriptUri(String testName) {
    return testBaseGcsDir + "/" + testName + "/script.py";
  }

  public String getResultsDirUri(String testName) {
    return testBaseGcsDir + "/" + testName + "/results";
  }
}
