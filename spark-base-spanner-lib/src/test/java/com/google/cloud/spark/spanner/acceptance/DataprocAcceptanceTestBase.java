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

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.dataproc.v1.*;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Instance;
import com.google.cloud.spanner.InstanceAdminClient;
import com.google.cloud.spanner.InstanceConfig;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.InstanceInfo;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spark.spanner.TestData;
import com.google.common.base.Preconditions;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import com.google.spanner.admin.instance.v1.CreateInstanceMetadata;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataprocAcceptanceTestBase {

  public static final String REGION = "us-central1";
  public static final String DATAPROC_ENDPOINT = REGION + "-dataproc.googleapis.com:443";
  public static final String PROJECT_ID =
      Preconditions.checkNotNull(
          System.getenv("GOOGLE_CLOUD_PROJECT"),
          "Please set the 'GOOGLE_CLOUD_PROJECT' environment variable");
  private static final String DATABASE_ID =
      Preconditions.checkNotNull(
              System.getenv("SPANNER_DATABASE_ID"),
              "Please set the 'SPANNER_DATABASE_ID' environment variable")
          + "-"
          + System.nanoTime();
  private static final String INSTANCE_ID =
      Preconditions.checkNotNull(
          System.getenv("SPANNER_INSTANCE_ID"),
          "Please set the 'SPANNER_INSTANCE_ID' environment variable");
  private static final String TABLE = "ATable";
  private static Spanner spanner =
      SpannerOptions.newBuilder().setProjectId(PROJECT_ID).build().getService();
  private static final Logger logger = LoggerFactory.getLogger(DataprocAcceptanceTestBase.class);

  private AcceptanceTestContext context;

  public DataprocAcceptanceTestBase(AcceptanceTestContext context) {
    this.context = context;
  }

  @Test
  public void testRead() throws Exception {
    String testName = "test-read";
    Job result =
        createAndRunPythonJob(
            testName,
            "read_test_table.py",
            null,
            Arrays.asList(context.getResultsDirUri(testName), PROJECT_ID, INSTANCE_ID, DATABASE_ID),
            120);
    assertThat(result.getStatus().getState()).isEqualTo(JobStatus.State.DONE);
    String output = AcceptanceTestUtils.getCsv(context.getResultsDirUri(testName));
    assertThat(output.trim()).isEqualTo("41");
  }

  private Job createAndRunPythonJob(
      String testName, String pythonFile, String pythonZipUri, List<String> args, long duration)
      throws Exception {
    AcceptanceTestUtils.uploadToGcs(
        getClass().getResourceAsStream("/acceptance/" + pythonFile),
        context.getScriptUri(testName),
        "text/x-python");

    Job job =
        Job.newBuilder()
            .setPlacement(JobPlacement.newBuilder().setClusterName(context.clusterId))
            .setPysparkJob(createPySparkJobBuilder(testName, pythonZipUri, args))
            .build();

    return runAndWait(job, Duration.ofSeconds(duration));
  }

  private PySparkJob.Builder createPySparkJobBuilder(
      String testName, String pythonZipUri, List<String> args) {
    PySparkJob.Builder builder =
        PySparkJob.newBuilder()
            .setMainPythonFileUri(context.getScriptUri(testName))
            .addJarFileUris(context.connectorJarUri);

    if (pythonZipUri != null && pythonZipUri.length() != 0) {
      builder.addPythonFileUris(pythonZipUri);
      builder.addFileUris(pythonZipUri);
    }

    if (args != null && args.size() != 0) {
      builder.addAllArgs(args);
    }

    return builder;
  }

  private Job runAndWait(Job job, Duration timeout) throws Exception {
    try (JobControllerClient jobControllerClient =
        JobControllerClient.create(
            JobControllerSettings.newBuilder().setEndpoint(DATAPROC_ENDPOINT).build())) {
      Job request = jobControllerClient.submitJob(PROJECT_ID, REGION, job);
      String jobId = request.getReference().getJobId();
      CompletableFuture<Job> finishedJobFuture =
          CompletableFuture.supplyAsync(
              () -> waitForJobCompletion(jobControllerClient, PROJECT_ID, REGION, jobId));
      Job jobInfo = finishedJobFuture.get(timeout.getSeconds(), TimeUnit.SECONDS);
      logger.debug(jobInfo.toString());
      return jobInfo;
    }
  }

  Job waitForJobCompletion(
      JobControllerClient jobControllerClient, String projectId, String region, String jobId) {
    while (true) {
      // Poll the service periodically until the Job is in a finished state.
      Job jobInfo = jobControllerClient.getJob(projectId, region, jobId);
      switch (jobInfo.getStatus().getState()) {
        case DONE:
        case CANCELLED:
        case ERROR:
          return jobInfo;
        default:
          try {
            // Wait a second in between polling attempts.
            TimeUnit.SECONDS.sleep(1);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
      }
    }
  }

  protected static AcceptanceTestContext setup(
      String dataprocImageVersion,
      String connectorJarDirectory,
      String connectorJarPrefix,
      List<ClusterProperty> clusterProperties)
      throws Exception {
    String clusterPropertiesMarkers =
        clusterProperties.isEmpty()
            ? ""
            : clusterProperties.stream()
                .map(ClusterProperty::getMarker)
                .collect(Collectors.joining("-", "-", ""));
    String testId =
        String.format(
            "%s-%s%s%s",
            System.nanoTime(),
            dataprocImageVersion.charAt(0),
            dataprocImageVersion.charAt(2),
            clusterPropertiesMarkers);
    Map<String, String> properties =
        clusterProperties.stream()
            .collect(Collectors.toMap(ClusterProperty::getKey, ClusterProperty::getValue));
    String testBaseGcsDir = AcceptanceTestUtils.createTestBaseGcsDir(testId);
    String connectorJarUri = testBaseGcsDir + "/connector.jar";
    AcceptanceTestUtils.uploadConnectorJar(
        connectorJarDirectory, connectorJarPrefix, connectorJarUri);

    String clusterName =
        createClusterIfNeeded(dataprocImageVersion, testId, properties, connectorJarUri);
    AcceptanceTestContext acceptanceTestContext =
        new AcceptanceTestContext(testId, clusterName, testBaseGcsDir, connectorJarUri);
    createSpannerDataset();
    return acceptanceTestContext;
  }

  protected static void tearDown(AcceptanceTestContext context) throws Exception {
    if (context != null) {
      terminateCluster(context.clusterId);
      AcceptanceTestUtils.deleteGcsDir(context.testBaseGcsDir);
      deleteSpannerDatasetAndTables();
    }
  }

  protected static void createSpannerDataset() throws Exception {
    // 1. Create the Spanner instance.
    InstanceAdminClient instanceAdminClient = spanner.getInstanceAdminClient();
    InstanceConfig config =
        instanceAdminClient.listInstanceConfigs().iterateAll().iterator().next();
    InstanceInfo instanceInfo =
        InstanceInfo.newBuilder(InstanceId.of(PROJECT_ID, INSTANCE_ID))
            .setInstanceConfigId(config.getId())
            .setNodeCount(1)
            .setDisplayName("Spark Spanner Acceptance Test")
            .build();
    OperationFuture<Instance, CreateInstanceMetadata> createInstanceOperation =
        instanceAdminClient.createInstance(instanceInfo);

    try {
      createInstanceOperation.get();
    } catch (Exception e) {
      if (!e.toString().contains("ALREADY_EXISTS")) {
        throw e;
      }
    }

    DatabaseAdminClient databaseAdminClient = spanner.getDatabaseAdminClient();

    // 2. Create the database.
    OperationFuture<Database, CreateDatabaseMetadata> createDatabaseOperation =
        databaseAdminClient.createDatabase(INSTANCE_ID, DATABASE_ID, TestData.initialDDL);
    try {
      createDatabaseOperation.get();
    } catch (Exception e) {
      if (!e.toString().contains("ALREADY_EXISTS")) {
        throw e;
      }
    }

    // 3. Insert data into the databse.
    DatabaseClient databaseClient =
        spanner.getDatabaseClient(DatabaseId.of(PROJECT_ID, INSTANCE_ID, DATABASE_ID));

    databaseClient
        .readWriteTransaction()
        .run(
            txn -> {
              try {
                TestData.initialDML.forEach(sql -> txn.executeUpdate(Statement.of(sql)));
              } catch (Exception e) {
                if (!e.toString().contains("ALREADY_EXISTS")) {
                  throw e;
                }
              }
              return null;
            });
  }

  protected static void deleteSpannerDatasetAndTables() {
    DatabaseAdminClient databaseAdminClient = spanner.getDatabaseAdminClient();
    databaseAdminClient.dropDatabase(INSTANCE_ID, DATABASE_ID);
  }

  private static void cluster(ThrowingConsumer<ClusterControllerClient> command) throws Exception {
    try (ClusterControllerClient clusterControllerClient =
        ClusterControllerClient.create(
            ClusterControllerSettings.newBuilder().setEndpoint(DATAPROC_ENDPOINT).build())) {
      command.accept(clusterControllerClient);
    }
  }

  protected static void terminateCluster(String clusterName) throws Exception {
    cluster(client -> client.deleteClusterAsync(PROJECT_ID, REGION, clusterName).get());
  }

  protected static String createClusterIfNeeded(
      String dataprocImageVersion,
      String testId,
      Map<String, String> properties,
      String connectorJarUri)
      throws Exception {
    String clusterName = generateClusterName(testId);
    System.out.printf(
        "createClusterIfNeeded clusterName: %s, testId: %s, dataprocImageVersion: %s, connectorJarUri: %s, PROJECT_ID: %s, REGION: %s%n",
        clusterName, testId, dataprocImageVersion, connectorJarUri, PROJECT_ID, REGION);
    cluster(
        client ->
            client
                .createClusterAsync(
                    PROJECT_ID,
                    REGION,
                    createCluster(clusterName, dataprocImageVersion, properties, connectorJarUri))
                .get());
    return clusterName;
  }

  private static Cluster createCluster(
      String clusterName,
      String dataprocImageVersion,
      Map<String, String> properties,
      String connectorJarUri) {
    return Cluster.newBuilder()
        .setClusterName(clusterName)
        .setProjectId(PROJECT_ID)
        .setConfig(
            ClusterConfig.newBuilder()
                .setGceClusterConfig(
                    GceClusterConfig.newBuilder()
                        .setNetworkUri("default")
                        .addServiceAccountScopes("https://www.googleapis.com/auth/cloud-platform")
                        .setZoneUri(REGION + "-a"))
                .setMasterConfig(
                    InstanceGroupConfig.newBuilder()
                        .setNumInstances(1)
                        .setMachineTypeUri("n1-standard-4")
                        .setDiskConfig(
                            DiskConfig.newBuilder()
                                .setBootDiskType("pd-standard")
                                .setBootDiskSizeGb(300)
                                .setNumLocalSsds(0)))
                .setWorkerConfig(
                    InstanceGroupConfig.newBuilder()
                        .setNumInstances(2)
                        .setMachineTypeUri("n1-standard-4")
                        .setDiskConfig(
                            DiskConfig.newBuilder()
                                .setBootDiskType("pd-standard")
                                .setBootDiskSizeGb(300)
                                .setNumLocalSsds(0)))
                .setSoftwareConfig(
                    SoftwareConfig.newBuilder()
                        .setImageVersion(dataprocImageVersion)
                        .putAllProperties(properties)))
        .build();
  }

  public static String generateClusterName(String testId) {
    return String.format("spanner-connector-acceptance-%s", testId);
  }

  @FunctionalInterface
  private interface ThrowingConsumer<T> {
    void accept(T t) throws Exception;
  }

  protected static class ClusterProperty {
    private String key;
    private String value;
    private String marker;

    private ClusterProperty(String key, String value, String marker) {
      this.key = key;
      this.value = value;
      this.marker = marker;
    }

    protected static ClusterProperty of(String key, String value, String marker) {
      return new ClusterProperty(key, value, marker);
    }

    public String getKey() {
      return key;
    }

    public String getValue() {
      return value;
    }

    public String getMarker() {
      return marker;
    }
  }
}
