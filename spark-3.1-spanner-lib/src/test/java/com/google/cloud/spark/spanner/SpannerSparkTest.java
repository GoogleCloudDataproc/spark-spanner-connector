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
public class SpannerSparkTest {

  String databaseId = "spark-db";
  String instanceId = "spanner-spark";
  String projectId = "orijtech-161805";

  @Before
  public void setUp() throws Exception {
    SpannerOptions opts = SpannerOptions.newBuilder().build();
    Spanner spanner = opts.getService();
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
      e.printStackTrace();
    }

    DatabaseAdminClient dbAdminClient = spanner.getDatabaseAdminClient();
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
                    + " D TIMESTAMP\n"
                    + ") PRIMARY KEY(A)"));
    try {
      dop.get();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @After
  public void cleanupDB() {}

  @Test
  public void testSpannerTable() {
    Map<String, String> props = new HashMap<>();
    props.put("databaseId", databaseId);
    props.put("instanceId", instanceId);
    props.put("projectId", projectId);
    props.put("table", "ATable");

    SpannerTable sp = new SpannerTable(props);
    StructType gotSchema = sp.schema();
    StructType wantSchema =
        new StructType(
            Arrays.asList(
                    new StructField("A", DataTypes.LongType, true, null),
                    new StructField("B", DataTypes.StringType, true, null),
                    new StructField("C", DataTypes.BinaryType, true, null),
                    new StructField("D", DataTypes.TimestampType, true, null))
                .toArray(new StructField[4]));

    assertEquals(gotSchema, wantSchema);
  }
}
