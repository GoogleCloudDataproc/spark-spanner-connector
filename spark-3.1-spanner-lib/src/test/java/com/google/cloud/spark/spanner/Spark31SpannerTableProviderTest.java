package com.google.cloud.spark.spanner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Map;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

public class Spark31SpannerTableProviderTest extends SparkSpannerIntegrationTestBase {

  @Test
  public void getTableForWrite_withenablePartialRowUpdates_returnsTableWithDataFrameSchema() {
    // Arrange
    Spark31SpannerTableProvider provider = new Spark31SpannerTableProvider();
    Map<String, String> props = connectionProperties();
    props.put("table", TestData.WRITE_TABLE_NAME);
    props.put("enablePartialRowUpdates", "true");

    StructType partialSchema = new StructType().add("long_col", DataTypes.LongType, false);

    // Act
    Table table = provider.getTable(partialSchema, null, props);

    // Assert
    // The table schema should be the partial schema from the DataFrame.
    assertEquals(
        "With enablePartialRowUpdates=true, the table schema should match the DataFrame schema.",
        partialSchema,
        table.schema());
  }

  @Test
  public void getTableForWrite_withoutenablePartialRowUpdates_returnsTableWithFullSchema() {
    // Arrange
    Spark31SpannerTableProvider provider = new Spark31SpannerTableProvider();
    Map<String, String> props = connectionProperties();
    props.put("table", TestData.WRITE_TABLE_NAME);
    // enablePartialRowUpdates is not set.

    StructType partialSchema = new StructType().add("long_col", DataTypes.LongType, false);

    // Act
    Table table = provider.getTable(partialSchema, null, props);

    // Assert
    // The table schema should NOT be the partial schema.
    // It should be the full schema from the database.
    assertNotEquals(
        "Without enablePartialRowUpdates, the table schema should be the full database schema, not the partial DataFrame schema.",
        partialSchema,
        table.schema());

    // Verify it's the full schema by checking for a column that isn't in the partial schema.
    boolean foundExtraColumn =
        Arrays.stream(table.schema().fields())
            .anyMatch(f -> f.name().equalsIgnoreCase("string_col"));
    assertTrue(
        "Expected full schema containing 'string_col', but it was not found.", foundExtraColumn);
  }
}
