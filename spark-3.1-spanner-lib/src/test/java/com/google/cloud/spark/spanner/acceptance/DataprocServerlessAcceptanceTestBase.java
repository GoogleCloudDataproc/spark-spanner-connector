package com.google.cloud.spark.spanner.acceptance;

import static com.google.common.truth.Truth.assertThat;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.gax.longrunning.OperationSnapshot;
import com.google.cloud.dataproc.v1.*;
import com.google.cloud.dataproc.v1.Batch;
import com.google.cloud.dataproc.v1.BatchControllerClient;
import com.google.cloud.dataproc.v1.BatchControllerSettings;
import com.google.cloud.dataproc.v1.BatchOperationMetadata;
import com.google.cloud.dataproc.v1.CreateBatchRequest;
import com.google.cloud.dataproc.v1.PySparkBatch;
import com.google.cloud.dataproc.v1.RuntimeConfig;
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
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * The acceptance test on the Dataproc Serverless. The test have to be running on the project with
 * requireOsLogin disabled, otherwise an org policy violation error will be thrown.
 */
public class DataprocServerlessAcceptanceTestBase {
  public static final String CONNECTOR_JAR_DIRECTORY = "../spark-3.1-spanner/target";
  public static final String REGION = "us-central1";
  public static final String DATAPROC_ENDPOINT = REGION + "-dataproc.googleapis.com:443";
  public static final String PROJECT_ID =
      Preconditions.checkNotNull(
          System.getenv("GOOGLE_CLOUD_PROJECT"),
          "Please set the 'GOOGLE_CLOUD_PROJECT' environment variable");
  private static final String DATABASE_ID =
      Preconditions.checkNotNull(
          System.getenv("SPANNER_DATABASE_ID"),
          "Please set the 'SPANNER_DATABASE_ID' environment variable");
  private static final String INSTANCE_ID =
      Preconditions.checkNotNull(
          System.getenv("SPANNER_INSTANCE_ID"),
          "Please set the 'SPANNER_INSTANCE_ID' environment variable");
  protected static final long SERVERLESS_BATCH_TIMEOUT_IN_SECONDS = 600;
  private static final String TABLE = "ATable";
  private static Spanner spanner =
      SpannerOptions.newBuilder().setProjectId(PROJECT_ID).build().getService();

  BatchControllerClient batchController;
  String testName =
      getClass()
          .getSimpleName()
          .substring(0, getClass().getSimpleName().length() - 32)
          .toLowerCase(Locale.ENGLISH);
  String testId = String.format("%s-%s", testName, System.currentTimeMillis());
  String testBaseGcsDir = AcceptanceTestUtils.createTestBaseGcsDir(testId);
  String connectorJarUri = testBaseGcsDir + "/connector.jar";
  AcceptanceTestContext context =
      new AcceptanceTestContext(
          testId, generateClusterName(testId), testBaseGcsDir, connectorJarUri);

  private final String s8sImageVersion;
  private final String connectorJarPrefix;

  public DataprocServerlessAcceptanceTestBase(String connectorJarPrefix, String s8sImageVersion) {
    this.connectorJarPrefix = connectorJarPrefix;
    this.s8sImageVersion = s8sImageVersion;
  }

  @Before
  public void createBatchControllerClient() throws Exception {
    AcceptanceTestUtils.uploadConnectorJar(
        CONNECTOR_JAR_DIRECTORY, connectorJarPrefix, context.connectorJarUri);
    createSpannerDataset();

    batchController =
        BatchControllerClient.create(
            BatchControllerSettings.newBuilder().setEndpoint(DATAPROC_ENDPOINT).build());
  }

  @After
  public void tearDown() throws Exception {
    batchController.close();
    AcceptanceTestUtils.deleteGcsDir(context.testBaseGcsDir);
    deleteSpannerDatasetAndTables();
  }

  @Test
  public void testBatch() throws Exception {
    OperationSnapshot operationSnapshot =
        createAndRunPythonBatch(
            context,
            testName,
            "read_test_table.py",
            null,
            Arrays.asList(
                context.getResultsDirUri(testName), PROJECT_ID, INSTANCE_ID, DATABASE_ID));
    assertThat(operationSnapshot.isDone()).isTrue();
    assertThat(operationSnapshot.getErrorMessage()).isEmpty();
    String output = AcceptanceTestUtils.getCsv(context.getResultsDirUri(testName));
    assertThat(output.trim()).isEqualTo("41");
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

  protected OperationSnapshot createAndRunPythonBatch(
      AcceptanceTestContext context,
      String testName,
      String pythonFile,
      String pythonZipUri,
      List<String> args)
      throws Exception {
    AcceptanceTestUtils.uploadToGcs(
        DataprocServerlessAcceptanceTestBase.class.getResourceAsStream("/acceptance/" + pythonFile),
        context.getScriptUri(testName),
        "text/x-python");
    String parent = String.format("projects/%s/locations/%s", PROJECT_ID, REGION);
    Batch batch =
        Batch.newBuilder()
            .setName(parent + "/batches/" + context.clusterId)
            .setPysparkBatch(createPySparkBatchBuilder(context, testName, pythonZipUri, args))
            .setRuntimeConfig(RuntimeConfig.newBuilder().setVersion(s8sImageVersion))
            .build();

    OperationFuture<Batch, BatchOperationMetadata> batchAsync =
        batchController.createBatchAsync(
            CreateBatchRequest.newBuilder()
                .setParent(parent)
                .setBatchId(context.clusterId)
                .setBatch(batch)
                .build());

    return batchAsync.getPollingFuture().get(SERVERLESS_BATCH_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS);
  }

  protected PySparkBatch.Builder createPySparkBatchBuilder(
      AcceptanceTestContext context, String testName, String pythonZipUri, List<String> args) {
    PySparkBatch.Builder builder =
        PySparkBatch.newBuilder()
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

  public static String generateClusterName(String testId) {
    return String.format("spanner-connector-serverless-acceptance-%s", testId);
  }
}
