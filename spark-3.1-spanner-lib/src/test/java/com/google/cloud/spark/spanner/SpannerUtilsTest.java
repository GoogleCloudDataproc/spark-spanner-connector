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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
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
    StructType dfSchema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.LongType, false, null),
              new StructField("name", DataTypes.StringType, true, null),
              new StructField("value", DataTypes.DoubleType, true, null)
            });

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
}
