package com.google.cloud.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.CreateDatabaseMetadata;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SpannerSparkTest {

  String instanceId = "ins";
  String databaseId = "db";

  @Before
  public void setUp() throws Exception {
    SpannerOptions opts = SpannerOptions.newBuilder().build();
    Spanner spanner = opts.getService();
    DatabaseAdminClient dbAdminClient = spanner.getDatabaseAdminClient();
    // 1. Setup the tables with the Cloud Spanner emulator.
    OperationFuture<Database, CreateDatabaseMetadata> op =
        dbAdminClient.createDatabase(
            instanceId,
            databaseId,
            ArrayList.asList(
                "CREATE TABLE ATable (\n"
                    + " A INT64 NOT NULL,\n"
                    + " B STRING(100),\n"
                    + " C BYTES(MAX),\n"
                    + " D TIMESTAMP\n"
                    + ") PRIMARY KEY(A)"));
    assertThatCode(
            () -> {
              op.waitFor().getResult();
            })
        .doesNotThrowAnyException();
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
            ArrayList.toArray(
                new StructField("A", DataTypes.LongType, true, null),
                new StructField("B", DataTypes.StringType, true, null),
                new StructField("C", DataTypes.StringType, true, null),
                new StructField("D", DataTypes.TimestampType, true, null)));

    assertThat(gotSchema).isEqualTo(wantSchema);
  }
}
