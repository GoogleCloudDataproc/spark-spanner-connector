package com.google.cloud.spark.spanner.scan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spark.spanner.SpannerConnectorException;
import com.google.cloud.spark.spanner.SpannerErrorCode;
import com.google.cloud.spark.spanner.SpannerUtils;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeAnnotationCode;
import com.google.spanner.v1.TypeCode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

public class SpannerQueryTableTest {

  @Test
  public void validateQueryAcceptsSelectAndCommonTableExpressions() {
    SpannerQueryTable.validateQuery(
        Statement.of("SELECT id FROM Users"), Dialect.GOOGLE_STANDARD_SQL);
    SpannerQueryTable.validateQuery(
        Statement.of("WITH users AS (SELECT 1 AS id) SELECT id FROM users"),
        Dialect.GOOGLE_STANDARD_SQL);
    SpannerQueryTable.validateQuery(Statement.of("SELECT id FROM users"), Dialect.POSTGRESQL);
  }

  @Test
  public void validateQueryRejectsMutations() {
    SpannerConnectorException error =
        assertThrows(
            SpannerConnectorException.class,
            () ->
                SpannerQueryTable.validateQuery(
                    Statement.of("DELETE FROM Users WHERE true"), Dialect.GOOGLE_STANDARD_SQL));

    assertEquals(SpannerErrorCode.INVALID_ARGUMENT, error.getErrorCode());
  }

  @Test
  public void resultSetMetadataToSchemaConvertsScalarTypes() {
    ResultSetMetadata metadata =
        metadata(
            field("enabled", type(TypeCode.BOOL)),
            field("payload", type(TypeCode.BYTES)),
            field("created", type(TypeCode.DATE)),
            field("score", type(TypeCode.FLOAT64)),
            field("id", type(TypeCode.INT64)),
            field("amount", type(TypeCode.NUMERIC)),
            field("name", type(TypeCode.STRING)),
            field("updated", type(TypeCode.TIMESTAMP)));

    StructType schema = SpannerQueryTable.resultSetMetadataToSchema(metadata);

    assertEquals(DataTypes.BooleanType, schema.apply("enabled").dataType());
    assertEquals(DataTypes.BinaryType, schema.apply("payload").dataType());
    assertEquals(DataTypes.DateType, schema.apply("created").dataType());
    assertEquals(DataTypes.DoubleType, schema.apply("score").dataType());
    assertEquals(DataTypes.LongType, schema.apply("id").dataType());
    assertEquals(DataTypes.createDecimalType(38, 9), schema.apply("amount").dataType());
    assertEquals(DataTypes.StringType, schema.apply("name").dataType());
    assertEquals(DataTypes.TimestampType, schema.apply("updated").dataType());
    assertTrue(schema.apply("id").nullable());
  }

  @Test
  public void resultSetMetadataToSchemaConvertsArraysAndStructs() {
    Type arrayType =
        Type.newBuilder().setCode(TypeCode.ARRAY).setArrayElementType(type(TypeCode.INT64)).build();
    Type structType =
        Type.newBuilder()
            .setCode(TypeCode.STRUCT)
            .setStructType(
                com.google.spanner.v1.StructType.newBuilder()
                    .addFields(field("name", type(TypeCode.STRING)))
                    .addFields(field("count", type(TypeCode.INT64))))
            .build();

    StructType schema =
        SpannerQueryTable.resultSetMetadataToSchema(
            metadata(field("values", arrayType), field("details", structType)));

    assertEquals(
        DataTypes.createArrayType(DataTypes.LongType, true), schema.apply("values").dataType());
    StructType details = (StructType) schema.apply("details").dataType();
    assertEquals(DataTypes.StringType, details.apply("name").dataType());
    assertEquals(DataTypes.LongType, details.apply("count").dataType());
  }

  @Test
  public void resultSetMetadataToSchemaPreservesJsonAnnotations() {
    Type jsonbType =
        Type.newBuilder()
            .setCode(TypeCode.JSON)
            .setTypeAnnotation(TypeAnnotationCode.PG_JSONB)
            .build();
    StructType schema =
        SpannerQueryTable.resultSetMetadataToSchema(
            metadata(field("json", type(TypeCode.JSON)), field("jsonb", jsonbType)));

    assertEquals("json", schema.apply("json").metadata().getString(SpannerUtils.COLUMN_TYPE));
    assertEquals("jsonb", schema.apply("jsonb").metadata().getString(SpannerUtils.COLUMN_TYPE));
  }

  @Test
  public void resultSetMetadataToSchemaRejectsUnnamedColumns() {
    SpannerConnectorException error =
        assertThrows(
            SpannerConnectorException.class,
            () ->
                SpannerQueryTable.resultSetMetadataToSchema(
                    metadata(field("", type(TypeCode.INT64)))));

    assertTrue(error.getMessage().contains("alias"));
  }

  @Test
  public void resultSetMetadataToSchemaRejectsDuplicateColumnsCaseInsensitively() {
    SpannerConnectorException error =
        assertThrows(
            SpannerConnectorException.class,
            () ->
                SpannerQueryTable.resultSetMetadataToSchema(
                    metadata(
                        field("id", type(TypeCode.INT64)), field("ID", type(TypeCode.INT64)))));

    assertTrue(error.getMessage().contains("Duplicate"));
  }

  @Test
  public void resultSetMetadataToSchemaRejectsUnsupportedTypes() {
    SpannerConnectorException error =
        assertThrows(
            SpannerConnectorException.class,
            () ->
                SpannerQueryTable.resultSetMetadataToSchema(
                    metadata(field("value", type(TypeCode.PROTO)))));

    assertEquals(SpannerErrorCode.UNSUPPORTED_DATATYPE, error.getErrorCode());
  }

  private static ResultSetMetadata metadata(com.google.spanner.v1.StructType.Field... fields) {
    com.google.spanner.v1.StructType.Builder rowType =
        com.google.spanner.v1.StructType.newBuilder();
    for (com.google.spanner.v1.StructType.Field field : fields) {
      rowType.addFields(field);
    }
    return ResultSetMetadata.newBuilder().setRowType(rowType).build();
  }

  private static com.google.spanner.v1.StructType.Field field(String name, Type type) {
    return com.google.spanner.v1.StructType.Field.newBuilder().setName(name).setType(type).build();
  }

  private static Type type(TypeCode code) {
    return Type.newBuilder().setCode(code).build();
  }
}
