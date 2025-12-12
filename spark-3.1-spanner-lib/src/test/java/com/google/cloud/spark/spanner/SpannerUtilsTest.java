package com.google.cloud.spark.spanner;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
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
}
