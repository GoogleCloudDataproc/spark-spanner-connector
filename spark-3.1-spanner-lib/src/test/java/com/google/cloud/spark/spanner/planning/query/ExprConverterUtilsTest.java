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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.cloud.spark.spanner.planning.expression.LiteralExpr;
import java.util.Collections;
import org.apache.spark.sql.types.DataTypes;
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
}
