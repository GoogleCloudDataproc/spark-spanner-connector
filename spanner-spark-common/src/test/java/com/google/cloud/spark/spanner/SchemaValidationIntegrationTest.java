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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import java.util.Arrays;
import java.util.Collection;
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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public abstract class SchemaValidationIntegrationTest extends SparkSpannerIntegrationTestBase {

  private final boolean usePostgreSql;
  private final String SCHEMA_VALIDATION_TABLE_NAME = "schema_test_table";

  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {{false}, {true}});
  }

  public SchemaValidationIntegrationTest(boolean usePostgreSql) {
    super();
    this.usePostgreSql = usePostgreSql;
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

    Map<String, String> props = connectionProperties(usePostgreSql);
    props.put("table", SCHEMA_VALIDATION_TABLE_NAME);
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

    Map<String, String> props = connectionProperties(usePostgreSql);
    props.put("table", SCHEMA_VALIDATION_TABLE_NAME);
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

    Map<String, String> props = connectionProperties(usePostgreSql);
    props.put("table", SCHEMA_VALIDATION_TABLE_NAME);
    // "enablePartialRowUpdates" is NOT set

    // Expect Spark's AnalysisException, not our custom SpannerConnectorException
    AnalysisException e =
        assertThrows(
            AnalysisException.class,
            () -> df.write().format("cloud-spanner").options(props).mode(SaveMode.Append).save());

    assertThat(e.getMessage()).contains("Cannot write incompatible data to table");
    assertThat(e.getMessage()).contains("Cannot find data for output column 'name'");
  }

  @Test
  public void testNumericScaleMismatchFailsInGoogleSql() {
    if (usePostgreSql) {
      // This validation is specific to GoogleSQL's strict NUMERIC(38,9) type.
      // PostgreSQL's NUMERIC is more flexible and this write may not fail.
      return;
    }

    // Create a DataFrame with a high-scale decimal type.
    StructType dfSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("long_col", DataTypes.LongType, false),
              DataTypes.createStructField("numeric_col", DataTypes.createDecimalType(38, 10), true)
            });
    List<Row> rows =
        Collections.singletonList(
            RowFactory.create(999L, new java.math.BigDecimal("123.1234567890")));
    Dataset<Row> df = spark.createDataFrame(rows, dfSchema);

    Map<String, String> props = connectionProperties(usePostgreSql);
    props.put("table", TestData.WRITE_TABLE_NAME);
    props.put("enablePartialRowUpdates", "true"); // Must be true to test connector validation.

    SpannerConnectorException e =
        assertThrows(
            SpannerConnectorException.class,
            () -> df.write().format("cloud-spanner").options(props).mode(SaveMode.Append).save());

    assertThat(e.getErrorCode()).isEqualTo(SpannerErrorCode.SCHEMA_VALIDATION_ERROR);
    assertThat(e.getMessage()).contains("Data type mismatch for column 'numeric_col'");
    assertThat(e.getMessage())
        .contains("DataFrame has type decimal(38,10) but Spanner table expects type decimal(38,9)");
  }
}
