package com.google.cloud.spark;

import static org.junit.Assert.assertEquals;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.Instance;
import com.google.cloud.spanner.InstanceAdminClient;
import com.google.cloud.spanner.InstanceConfig;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.InstanceInfo;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spark.spanner.SpannerTable;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import com.google.spanner.admin.instance.v1.CreateInstanceMetadata;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SpannerTableTest {

  String databaseId = System.getenv("SPANNER_DATABASE_ID");
  String instanceId = System.getenv("SPANNER_INSTANCE_ID");
  String projectId = System.getenv("SPANNER_PROJECT_ID");
  String emulatorHost = System.getenv("SPANNER_EMULATOR_HOST");

  DatabaseAdminClient dbAdminClient;
  Spanner spanner;

  @Before
  public void setUp() throws Exception {
    SpannerOptions opts = SpannerOptions.newBuilder().setEmulatorHost(emulatorHost).build();
    spanner = opts.getService();
    // 1. Create the Spanner instance.
    // TODO: Skip this process if the instance already exists.
    InstanceAdminClient insAdminClient = spanner.getInstanceAdminClient();
    InstanceConfig config = insAdminClient.listInstanceConfigs().iterateAll().iterator().next();
    InstanceInfo insInfo =
        InstanceInfo.newBuilder(InstanceId.of(projectId, instanceId))
            .setInstanceConfigId(config.getId())
            .setNodeCount(1)
            .setDisplayName("SparkSpanner Test")
            .build();
    OperationFuture<Instance, CreateInstanceMetadata> iop = insAdminClient.createInstance(insInfo);

    try {
      iop.get();
    } catch (Exception e) {
      if (!e.toString().contains("ALREADY_EXISTS")) {
        throw e;
      }
    }

    dbAdminClient = spanner.getDatabaseAdminClient();
    // 2. Create the database.
    // TODO: Skip this process if the database already exists.
    OperationFuture<Database, CreateDatabaseMetadata> dop =
        dbAdminClient.createDatabase(
            instanceId,
            databaseId,
            Arrays.asList(
                "CREATE TABLE ATable (\n"
                    + " A INT64 NOT NULL,\n"
                    + " B STRING(100),\n"
                    + " C BYTES(MAX),\n"
                    + " D TIMESTAMP,\n"
                    + " E NUMERIC,\n"
                    + " F ARRAY<STRING(MAX)>\n"
                    + ") PRIMARY KEY(A)"));
    try {
      dop.get();
    } catch (Exception e) {
      if (!e.toString().contains("ALREADY_EXISTS")) {
        throw e;
      }
    }
  }

  @After
  public void teardown() {
    spanner.close();
  }

  private Map<String, String> connectionProperties() {
    Map<String, String> props = new HashMap<>();
    props.put("databaseId", databaseId);
    props.put("instanceId", instanceId);
    props.put("projectId", projectId);
    if (false) {
      props.put("emulatorHost", emulatorHost);
    }
    props.put("table", "ATable");
    return props;
  }

  @Test
  public void createSchema() {
    Map<String, String> props = connectionProperties();
    SpannerTable st = new SpannerTable(null, props);
    StructType actualSchema = st.schema();
    StructType expectSchema =
        new StructType(
            Arrays.asList(
                    new StructField("A", DataTypes.LongType, false, null),
                    new StructField("B", DataTypes.StringType, true, null),
                    new StructField(
                        "C", DataTypes.createArrayType(DataTypes.ByteType, true), true, null),
                    new StructField("D", DataTypes.TimestampType, true, null),
                    new StructField("E", DataTypes.createDecimalType(38, 9), true, null),
                    new StructField(
                        "F", DataTypes.createArrayType(DataTypes.StringType, true), true, null))
                .toArray(new StructField[0]));

    // Object.equals fails for StructType with fields so we'll
    // firstly compare lengths, then fieldNames then the simpleString.
    assertEquals(expectSchema.length(), actualSchema.length());
    assertEquals(expectSchema.fieldNames(), actualSchema.fieldNames());
    assertEquals(expectSchema.simpleString(), actualSchema.simpleString());
  }
}
