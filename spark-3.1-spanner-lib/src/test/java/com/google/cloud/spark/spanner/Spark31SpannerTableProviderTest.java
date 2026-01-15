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

package com.google.cloud.spark.spanner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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

  @Test
  public void getTableAllowsLowerCaseProperties() {
    // Arrange
    Spark31SpannerTableProvider provider = new Spark31SpannerTableProvider();
    Map<String, String> props = connectionPropertiesLowerCase(false);

    final StructType partialSchema = new StructType().add("long_col", DataTypes.LongType, false);

    // Act
    try {
      Table table = provider.getTable(partialSchema, null, props);
    } catch (SpannerConnectorException e) {
      // Assert
      fail("An unexpected exception was thrown: " + e.getMessage());
    }
  }
}
