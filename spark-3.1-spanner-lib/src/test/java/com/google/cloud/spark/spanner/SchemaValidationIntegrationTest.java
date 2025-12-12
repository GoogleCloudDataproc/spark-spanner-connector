package com.google.cloud.spark.spanner;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

public class SchemaValidationIntegrationTest extends SparkSpannerIntegrationTestBase {

  private final boolean usePg;

  public SchemaValidationIntegrationTest() {
    this(false);
  }

  protected SchemaValidationIntegrationTest(boolean usePg) {
    super();
    this.usePg = usePg;
  }

  @Test
  public void testColumnNotFound() {
    StructType dfSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.LongType, false),
              DataTypes.createStructField("non_existent_col", DataTypes.StringType, true)
            });
    List<Row> rows = Collections.singletonList(RowFactory.create(1L, "some_string"));
    Dataset<Row> df = spark.createDataFrame(rows, dfSchema);

    Map<String, String> props = connectionProperties(usePg);
    props.put("table", "schemaValidationTestTable");
    props.put("enablePartialRowUpdates", "true");

    SpannerConnectorException e =
        assertThrows(
            SpannerConnectorException.class,
            () -> df.write().format("cloud-spanner").options(props).mode(SaveMode.Append).save());

    assertThat(e.getErrorCode()).isEqualTo(SpannerErrorCode.SCHEMA_VALIDATION_ERROR);
    assertThat(e.getMessage()).contains("DataFrame column 'non_existent_col' not found");
  }

  @Test
  public void testDataTypeMismatch() {
    StructType dfSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.StringType, false) // wrong type
            });
    List<Row> rows = Collections.singletonList(RowFactory.create("not_a_long"));
    Dataset<Row> df = spark.createDataFrame(rows, dfSchema);

    Map<String, String> props = connectionProperties(usePg);
    props.put("table", "schemaValidationTestTable");
    props.put("enablePartialRowUpdates", "true");

    SpannerConnectorException e =
        assertThrows(
            SpannerConnectorException.class,
            () -> df.write().format("cloud-spanner").options(props).mode(SaveMode.Append).save());

    assertThat(e.getErrorCode()).isEqualTo(SpannerErrorCode.SCHEMA_VALIDATION_ERROR);
    assertThat(e.getMessage()).contains("Data type mismatch for column 'id'");
  }

  @Test
  public void testPartialWriteFailsWithoutOption() {
    StructType partialSchema =
        new StructType(
            new StructField[] {DataTypes.createStructField("id", DataTypes.LongType, false)});
    List<Row> rows = Collections.singletonList(RowFactory.create(1L));
    Dataset<Row> df = spark.createDataFrame(rows, partialSchema);

    Map<String, String> props = connectionProperties(usePg);
    props.put("table", "schemaValidationTestTable");
    // "enablePartialRowUpdates" is NOT set

    // Expect Spark's AnalysisException, not our custom SpannerConnectorException
    AnalysisException e =
        assertThrows(
            AnalysisException.class,
            () -> df.write().format("cloud-spanner").options(props).mode(SaveMode.Append).save());

    assertThat(e.getMessage()).contains("Cannot write incompatible data to table");
    assertThat(e.getMessage()).contains("Cannot find data for output column 'name'");
  }
}
