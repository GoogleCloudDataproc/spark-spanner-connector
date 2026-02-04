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

package com.google.cloud.spark.spanner.integration;

import static org.junit.Assert.*;

import com.google.cloud.spark.spanner.SparkSpannerTableProviderBase;
import com.google.cloud.spark.spanner.TestData;
import java.util.Arrays;
import java.util.Map;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

public abstract class SparkSpannerTableProviderIntegrationTestBase<
        T extends SparkSpannerTableProviderBase>
    extends SparkSpannerIntegrationTestBase {

  protected abstract T getInstance();

  @Test
  public void getTableForWrite_withenablePartialRowUpdates_returnsTableWithDataFrameSchema() {
    // Arrange
    T provider = getInstance();
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
    T provider = getInstance();
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
    T provider = getInstance();
    Map<String, String> props = connectionPropertiesLowerCase(false);

    final StructType partialSchema = new StructType().add("long_col", DataTypes.LongType, false);

    // Act
    Table table = provider.getTable(partialSchema, null, props);
    // Assert
    assertEquals("ATable", table.name());
    // enablePartialRowUpdates test.
    assertEquals(partialSchema, table.schema());
  }
}
