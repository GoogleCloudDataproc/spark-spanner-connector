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

import static com.google.cloud.spark.spanner.SpannerUtils.pruneSchema;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.Test;

public class SpannerUtilsTest {

  private static final String TABLE_NAME = "test_table";

  private StructType createSpannerSchema() {
    return new StructType(
        new StructField[] {
          new StructField("id", DataTypes.LongType, false, null),
          new StructField("name", DataTypes.StringType, true, null),
          new StructField("value", DataTypes.DoubleType, true, null)
        });
  }

  @Test
  public void testValidateSchemaSuccess() {
    StructType spannerSchema = createSpannerSchema();
    StructType dfSchema = createSpannerSchema();

    // Should not throw any exception
    SpannerUtils.validateSchema(dfSchema, spannerSchema, TABLE_NAME);
  }

  @Test
  public void testValidateSchemaSuccessSubsetAndOutOfOrder() {
    StructType spannerSchema = createSpannerSchema();
    StructType dfSchema =
        new StructType(
            new StructField[] {
              new StructField("name", DataTypes.StringType, true, null),
              new StructField("id", DataTypes.LongType, false, null)
            });

    // Should not throw any exception
    SpannerUtils.validateSchema(dfSchema, spannerSchema, TABLE_NAME);
  }

  @Test
  public void testValidateSchemaSuccessCaseInsensitive() {
    StructType spannerSchema = createSpannerSchema();
    StructType dfSchema =
        new StructType(
            new StructField[] {
              new StructField("VALUE", DataTypes.DoubleType, true, null),
              new StructField("Id", DataTypes.LongType, false, null)
            });

    // Should not throw any exception
    SpannerUtils.validateSchema(dfSchema, spannerSchema, TABLE_NAME);
  }

  @Test
  public void testValidateSchemaColumnNotFound() {
    StructType spannerSchema = createSpannerSchema();
    StructType dfSchema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.LongType, false, null),
              new StructField("non_existent_col", DataTypes.StringType, true, null)
            });

    SpannerConnectorException exception =
        assertThrows(
            SpannerConnectorException.class,
            () -> SpannerUtils.validateSchema(dfSchema, spannerSchema, TABLE_NAME));
    assertTrue(exception.getMessage().contains("DataFrame column 'non_existent_col' not found"));
  }

  @Test
  public void testValidateSchemaDataTypeMismatch() {
    StructType spannerSchema = createSpannerSchema();
    StructType dfSchema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.StringType, false, null) // incorrect type
            });

    SpannerConnectorException exception =
        assertThrows(
            SpannerConnectorException.class,
            () -> SpannerUtils.validateSchema(dfSchema, spannerSchema, TABLE_NAME));
    assertTrue(exception.getMessage().contains("Data type mismatch for column 'id'"));
    assertTrue(
        exception
            .getMessage()
            .contains("DataFrame has type string but Spanner table expects type bigint"));
  }

  @Test
  public void testValidateSchemaFloat32VsFloat64Mismatch() {
    StructType spannerSchema =
        new StructType(
            new StructField[] {new StructField("score", DataTypes.FloatType, true, null)});
    StructType dfSchema =
        new StructType(
            new StructField[] {new StructField("score", DataTypes.DoubleType, true, null)});

    SpannerConnectorException exception =
        assertThrows(
            SpannerConnectorException.class,
            () -> SpannerUtils.validateSchema(dfSchema, spannerSchema, TABLE_NAME));
    assertTrue(exception.getMessage().contains("Data type mismatch for column 'score'"));
    assertTrue(
        exception
            .getMessage()
            .contains("DataFrame has type double but Spanner table expects type float"));
  }

  @Test
  public void testValidateSchemaNumericScaleMismatch() {
    // Spanner GoogleSQL NUMERIC maps to DecimalType(38, 9)
    StructType spannerSchema =
        new StructType(
            new StructField[] {
              new StructField("price", DataTypes.createDecimalType(38, 9), true, null)
            });

    // DataFrame has a numeric column with scale > 9
    StructType dfSchema =
        new StructType(
            new StructField[] {
              new StructField("price", DataTypes.createDecimalType(38, 10), true, null)
            });

    SpannerConnectorException exception =
        assertThrows(
            SpannerConnectorException.class,
            () -> SpannerUtils.validateSchema(dfSchema, spannerSchema, TABLE_NAME));
    assertTrue(exception.getMessage().contains("Data type mismatch for column 'price'"));
    assertTrue(
        exception
            .getMessage()
            .contains(
                "DataFrame has type decimal(38,10) but Spanner table expects type decimal(38,9)"));
  }

  @Test
  public void testPartialRowUpdatesSuccessEnablePartialRowUpdates() {
    StructType spannerSchema = createSpannerSchema();
    StructType dfSchema = createSpannerSchema();

    // Should not throw any exception
    SpannerUtils.validatePartialRowUpdates(dfSchema, spannerSchema, true);
  }

  @Test
  public void testPartialRowUpdatesSuccessDisablePartialRowUpdates() {
    StructType spannerSchema = createSpannerSchema();
    StructType dfSchema = createSpannerSchema();

    // Should not throw any exception
    SpannerUtils.validatePartialRowUpdates(dfSchema, spannerSchema, false);
  }

  @Test
  public void testPartialRowUpdatesValidateSkippedWhenDataFrameNull() {
    StructType spannerSchema = createSpannerSchema();

    // Should not throw any exception
    SpannerUtils.validatePartialRowUpdates(null, spannerSchema, true);
  }

  @Test
  public void
      testPartialRowUpdatesValidateSkippedWhenEnablePartialRowUpdatesTrueAndPartialDFSchema() {
    StructType spannerSchema = createSpannerSchema();
    StructType dfSchema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.LongType, false, null),
              new StructField("value", DataTypes.DoubleType, true, null)
            });

    // Should not throw any exception
    SpannerUtils.validatePartialRowUpdates(dfSchema, spannerSchema, true);
  }

  @Test
  public void testPartialRowUpdatesFailsWhenDisablePartialRowUpdatesAndPartialDFSchema() {
    StructType spannerSchema = createSpannerSchema();
    StructType dfSchema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.LongType, false, null),
              new StructField("value", DataTypes.DoubleType, true, null)
            });

    // Should throw an exception
    SpannerConnectorException exception =
        assertThrows(
            SpannerConnectorException.class,
            () -> SpannerUtils.validatePartialRowUpdates(dfSchema, spannerSchema, false));
    assertTrue(
        exception
            .getMessage()
            .contains("Partial row updates require enablePartialRowUpdates=true."));
  }

  @Test
  public void testGetRequiredOptionCaseInsensitive() {
    Map<String, String> props = new HashMap<>();
    props.put("TABLE", "my_table");
    props.put("instanceId", "my_instance");
    CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(props);

    assertEquals("my_table", SpannerUtils.getRequiredOption(options, "table"));
    assertEquals("my_table", SpannerUtils.getRequiredOption(options, "TABLE"));
    assertEquals("my_table", SpannerUtils.getRequiredOption(options, "Table"));

    assertEquals("my_instance", SpannerUtils.getRequiredOption(options, "instanceid"));
    assertEquals("my_instance", SpannerUtils.getRequiredOption(options, "INSTANCEID"));
    assertEquals("my_instance", SpannerUtils.getRequiredOption(options, "instanceId"));
  }

  @Test
  public void testGetRequiredOptionMissing() {
    Map<String, String> props = new HashMap<>();
    CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(props);

    SpannerConnectorException e =
        assertThrows(
            SpannerConnectorException.class,
            () -> SpannerUtils.getRequiredOption(options, "table"));
    assertTrue(e.getMessage().contains("Option 'table' property must be set"));
  }

  @Test
  public void testBuildSpannerOptionsPreservesDefaultCompression() {
    Map<String, String> props = new HashMap<>();
    props.put("projectId", "test-project");

    assertNull(
        SpannerUtils.buildSpannerOptions(new CaseInsensitiveStringMap(props)).getCompressorName());
  }

  @Test
  public void testBuildSpannerOptionsEnablesGrpcGzipCompression() {
    Map<String, String> props = new HashMap<>();
    props.put("projectId", "test-project");
    props.put("grpcCompression", "gzip");

    assertEquals(
        "gzip",
        SpannerUtils.buildSpannerOptions(new CaseInsensitiveStringMap(props)).getCompressorName());
  }

  @Test
  public void testBuildSpannerOptionsRejectsUnknownGrpcCompressor() {
    Map<String, String> props = new HashMap<>();
    props.put("projectId", "test-project");
    props.put("grpcCompression", "unknown");

    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () -> SpannerUtils.buildSpannerOptions(new CaseInsensitiveStringMap(props)));
    assertTrue(error.getMessage().contains("unknown is not a known compressor"));
  }

  @Test
  public void testValidateProjectIdAcceptsStandard() {
    SpannerUtils.validateProjectId("my-project-123");
  }

  @Test
  public void testValidateProjectIdAcceptsDomainScoped() {
    SpannerUtils.validateProjectId("example.com:my-project");
  }

  @Test
  public void testValidateProjectIdAcceptsNumeric() {
    SpannerUtils.validateProjectId("123456789");
  }

  @Test
  public void testValidateProjectIdRejectsUriInjection() {
    SpannerConnectorException e =
        assertThrows(
            SpannerConnectorException.class,
            () -> SpannerUtils.validateProjectId("proj?inject=true"));
    assertTrue(e.getMessage().contains("Invalid projectId"));
  }

  @Test
  public void testValidateResourceIdAcceptsValid() {
    SpannerUtils.validateResourceId("instance-1", "instanceId");
    SpannerUtils.validateResourceId("my_database", "databaseId");
  }

  @Test
  public void testValidateResourceIdRejectsUriInjection() {
    SpannerConnectorException e =
        assertThrows(
            SpannerConnectorException.class,
            () -> SpannerUtils.validateResourceId("db?autoConfigEmulator=true", "databaseId"));
    assertTrue(e.getMessage().contains("Invalid databaseId"));
  }

  @Test
  public void testValidateResourceIdRejectsSemicolonInjection() {
    SpannerConnectorException e =
        assertThrows(
            SpannerConnectorException.class,
            () -> SpannerUtils.validateResourceId("db;usePlainText=true", "databaseId"));
    assertTrue(e.getMessage().contains("Invalid databaseId"));
  }

  @Test
  public void testValidateEmulatorHostAcceptsValid() {
    SpannerUtils.validateEmulatorHost("localhost:9010");
    SpannerUtils.validateEmulatorHost("127.0.0.1:9010");
  }

  @Test
  public void testValidateEmulatorHostRejectsUriInjection() {
    SpannerConnectorException e =
        assertThrows(
            SpannerConnectorException.class,
            () -> SpannerUtils.validateEmulatorHost("localhost:9010/evil?param=val"));
    assertTrue(e.getMessage().contains("Invalid emulatorHost"));
  }

  @Test
  public void testDuplicateColumnsAreNotPruned() {
    StructType originalSchema = createSpannerSchema();
    List<String> requiredColumns = new LinkedList<>();
    requiredColumns.add("id");
    requiredColumns.add("value");
    requiredColumns.add("name");
    requiredColumns.add("id"); // duplicates first required column

    StructType expectedSchema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.LongType, false, null),
              new StructField("value", DataTypes.DoubleType, true, null),
              new StructField("name", DataTypes.StringType, true, null),
              new StructField("id", DataTypes.LongType, false, null)
            });

    StructType actualSchema = pruneSchema(originalSchema, requiredColumns);

    assertArrayEquals(expectedSchema.fields(), actualSchema.fields());
  }

  @Test
  public void testConvertFloat32Values() {
    Struct spannerRow =
        Struct.newBuilder()
            .set("value")
            .to(Value.float32(1.25f))
            .set("inexact")
            .to(Value.float32(-0.1f))
            .set("nan")
            .to(Value.float32(Float.NaN))
            .set("pos_inf")
            .to(Value.float32(Float.POSITIVE_INFINITY))
            .set("neg_inf")
            .to(Value.float32(Float.NEGATIVE_INFINITY))
            .set("max")
            .to(Value.float32(Float.MAX_VALUE))
            .set("min")
            .to(Value.float32(Float.MIN_VALUE))
            .set("null_value")
            .to(Value.float32(null))
            .build();

    InternalRow sparkRow = SpannerUtils.spannerStructToInternalRow(spannerRow);

    // FloatType values must be stored as floats, not doubles, for InternalRow.getFloat to work.
    assertEquals(1.25f, sparkRow.getFloat(0), 0.0f);
    assertEquals(-0.1f, sparkRow.getFloat(1), 0.0f);
    assertTrue(Float.isNaN(sparkRow.getFloat(2)));
    assertEquals(Float.POSITIVE_INFINITY, sparkRow.getFloat(3), 0.0f);
    assertEquals(Float.NEGATIVE_INFINITY, sparkRow.getFloat(4), 0.0f);
    assertEquals(Float.MAX_VALUE, sparkRow.getFloat(5), 0.0f);
    assertEquals(Float.MIN_VALUE, sparkRow.getFloat(6), 0.0f);
    assertTrue(sparkRow.isNullAt(7));
  }

  @Test
  public void testConvertFloat32ArrayValues() {
    Struct spannerRow =
        Struct.newBuilder()
            .set("values")
            .to(
                Value.float32Array(
                    Arrays.asList(
                        1.25f, null, Float.NaN, Float.NEGATIVE_INFINITY, Float.MAX_VALUE)))
            .set("empty")
            .to(Value.float32Array(new float[0]))
            .set("null_array")
            .to(Value.float32Array((float[]) null))
            .build();

    InternalRow sparkRow = SpannerUtils.spannerStructToInternalRow(spannerRow);

    ArrayData values = sparkRow.getArray(0);
    assertEquals(5, values.numElements());
    assertEquals(1.25f, values.getFloat(0), 0.0f);
    assertTrue(values.isNullAt(1));
    assertTrue(Float.isNaN(values.getFloat(2)));
    assertEquals(Float.NEGATIVE_INFINITY, values.getFloat(3), 0.0f);
    assertEquals(Float.MAX_VALUE, values.getFloat(4), 0.0f);
    assertEquals(0, sparkRow.getArray(1).numElements());
    assertTrue(sparkRow.isNullAt(2));
  }
}
