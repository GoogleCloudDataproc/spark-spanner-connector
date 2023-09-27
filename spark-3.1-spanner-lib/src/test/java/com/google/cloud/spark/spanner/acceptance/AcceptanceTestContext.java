package com.google.cloud.spark.spanner.acceptance;

/** */
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
