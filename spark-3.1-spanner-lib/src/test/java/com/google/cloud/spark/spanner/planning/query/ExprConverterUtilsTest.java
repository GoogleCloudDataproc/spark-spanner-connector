// Copyright 2026 Google LLC
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

package com.google.cloud.spark.spanner.planning.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.cloud.spark.spanner.planning.expression.LiteralExpr;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

public class ExprConverterUtilsTest {

  private static final String UUID_VALUE = "550e8400-e29b-41d4-a716-446655440000";

  @Test
  public void stringLiteralWithStringColumnRemainsString() {
    ColumnResolution resolution =
        new ColumnResolution("string_col", "string_col", "", DataTypes.StringType, "STRING", true);

    LiteralExpr result =
        ExprConverterUtils.toLiteral(
            UUID_VALUE, Collections.singletonMap("string_col", resolution), "string_col");

    assertEquals(UUID_VALUE, result.getValue());
    assertEquals(DataTypes.StringType, result.getSparkType());
    assertEquals("STRING", result.getSpannerType());
  }

  @Test
  public void uuidLookingStringWithStringColumnRemainsString() {
    ColumnResolution resolution =
        new ColumnResolution("string_col", "string_col", "", DataTypes.StringType, "STRING", true);

    LiteralExpr result =
        ExprConverterUtils.toLiteral(
            UUID_VALUE, Collections.singletonMap("string_col", resolution), "string_col");

    assertEquals(UUID_VALUE, result.getValue());
    assertEquals(DataTypes.StringType, result.getSparkType());
    assertEquals("STRING", result.getSpannerType());

    assertTrue(result.getValue() instanceof String);
  }

  @Test
  public void uuidLookingStringWithUuidColumnGetsUuidTargetType() {
    ColumnResolution resolution =
        new ColumnResolution("uuid_col", "uuid_col", "", DataTypes.StringType, "UUID", false);

    LiteralExpr result =
        ExprConverterUtils.toLiteral(
            UUID_VALUE, Collections.singletonMap("uuid_col", resolution), "uuid_col");

    assertEquals(UUID_VALUE, result.getValue());
    assertEquals(DataTypes.StringType, result.getSparkType());
    assertEquals("UUID", result.getSpannerType());

    // The value remains a String. The UUID conversion belongs in the binder.
    assertTrue(result.getValue() instanceof String);
  }

  @Test
  public void invalidUuidStringWithUuidColumnIsRejected() {
    ColumnResolution resolution =
        new ColumnResolution("uuid_col", "uuid_col", "", DataTypes.StringType, "UUID", false);

    try {
      ExprConverterUtils.toLiteral(
          "not-a-uuid", Collections.singletonMap("uuid_col", resolution), "uuid_col");

      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
      // Expected.
    }
  }

  @Test
  public void nonCanonicalUuidStringsWithUuidColumnAreRejected() {
    ColumnResolution resolution =
        new ColumnResolution("uuid_col", "uuid_col", "", DataTypes.StringType, "UUID", false);

    try {
      ExprConverterUtils.toLiteral(
          UUID_VALUE.toUpperCase(), Collections.singletonMap("uuid_col", resolution), "uuid_col");
      fail("Expected IllegalArgumentException for uppercase UUID");
    } catch (IllegalArgumentException expected) {
      // Expected.
    }

    try {
      ExprConverterUtils.toLiteral(
          UUID_VALUE.substring(0, 35),
          Collections.singletonMap("uuid_col", resolution),
          "uuid_col");
      fail("Expected IllegalArgumentException for 35-char UUID");
    } catch (IllegalArgumentException expected) {
      // Expected.
    }
  }

  @Test
  public void timestampLiteralsNormalizeToInstant() {
    Instant expected = Instant.parse("2026-01-01T00:00:00.123456Z");
    long epochMicros = expected.getEpochSecond() * 1_000_000L + 123_456L;

    assertEquals(
        expected, ExprConverterUtils.normalizeLiteral(epochMicros, DataTypes.TimestampType));
    assertEquals(
        expected,
        ExprConverterUtils.normalizeLiteral(Timestamp.from(expected), DataTypes.TimestampType));
    assertEquals(expected, ExprConverterUtils.normalizeLiteral(expected, DataTypes.TimestampType));
    assertEquals(
        Instant.ofEpochSecond(-1L, 999_999_000L),
        ExprConverterUtils.normalizeLiteral(-1L, DataTypes.TimestampType));
  }

  @Test
  public void nullTypeAndUnsupportedLiteralTypesAreRejected() {
    assertThrows(
        UnsupportedOperationException.class,
        () -> ExprConverterUtils.normalizeLiteral(null, DataTypes.NullType));
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            ExprConverterUtils.normalizeLiteral(
                new int[] {1}, DataTypes.createArrayType(DataTypes.IntegerType)));
  }

  @Test
  public void toColumnThrowsIllegalArgumentOnMissingResolutionAndRejectsUnsupportedTypes() {
    assertThrows(
        IllegalArgumentException.class,
        () -> ExprConverterUtils.toColumn("missing", Collections.emptyMap()));
    assertThrows(
        IllegalArgumentException.class, () -> ExprConverterUtils.toColumn("missing", null));

    StructType structType =
        new StructType(
            new StructField[] {
              new StructField("sub", DataTypes.StringType, true, Metadata.empty())
            });
    ColumnResolution structCol =
        new ColumnResolution("s", "s", "t", structType, "STRUCT<sub STRING>", true);
    ColumnResolution arrayCol =
        new ColumnResolution(
            "a", "a", "t", DataTypes.createArrayType(DataTypes.StringType), "ARRAY<STRING>", true);
    ColumnResolution jsonCol =
        new ColumnResolution("j", "j", "t", DataTypes.StringType, "JSON", true);
    ColumnResolution jsonbCol =
        new ColumnResolution("jb", "jb", "t", DataTypes.StringType, "jsonb", true);

    for (ColumnResolution unsupported :
        new ColumnResolution[] {structCol, arrayCol, jsonCol, jsonbCol}) {
      assertThrows(
          UnsupportedOperationException.class,
          () ->
              ExprConverterUtils.toColumn(
                  unsupported.getOutputName(),
                  Collections.singletonMap(unsupported.getOutputName(), unsupported)));
      assertThrows(
          UnsupportedOperationException.class,
          () -> ExprConverterUtils.toLiteral("val", unsupported));
    }
  }

  @Test
  public void toLiteralEnforcesGoogleSqlNumericBoundsOnlyOnGoogleSql() {
    ColumnResolution decCol =
        new ColumnResolution("d", "d", "t", DataTypes.createDecimalType(38, 12), "NUMERIC", true);
    BigDecimal highScale = new BigDecimal("0.0000000001");

    assertThrows(
        IllegalArgumentException.class,
        () -> ExprConverterUtils.toLiteral(highScale, decCol, false));

    LiteralExpr pgLiteral = ExprConverterUtils.toLiteral(highScale, decCol, true);
    assertEquals(highScale, pgLiteral.getValue());
  }
}
