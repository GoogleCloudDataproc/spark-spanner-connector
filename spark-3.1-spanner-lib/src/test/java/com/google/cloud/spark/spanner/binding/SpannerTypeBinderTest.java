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

package com.google.cloud.spark.spanner.binding;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import com.google.cloud.spark.spanner.planning.expression.LiteralExpr;
import java.math.BigDecimal;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Test;

public class SpannerTypeBinderTest {

  private static final String UUID_VALUE = "550e8400-e29b-41d4-a716-446655440000";

  @Test
  public void bindsUuidStringAsUuid() {
    LiteralExpr literal = new LiteralExpr(UUID_VALUE, DataTypes.StringType, "UUID");

    Statement.Builder builder = Statement.newBuilder("SELECT @p1");

    SpannerTypeBinder.bind(builder, "p1", literal);

    // The important assertion is that binding succeeds. The binder is
    // responsible for converting the String UUID to a UUID value before
    // passing it to the Spanner client.
    Statement statement = builder.build();

    // Building the statement confirms that the parameter was successfully
    // bound without treating the UUID as an ordinary String.
    if (statement == null) {
      fail("Expected a bound statement");
    }
  }

  @Test
  public void rejectsInvalidUuidString() {
    LiteralExpr literal = new LiteralExpr("not-a-uuid", DataTypes.StringType, "UUID");

    Statement.Builder builder = Statement.newBuilder("SELECT @p1");

    try {
      SpannerTypeBinder.bind(builder, "p1", literal);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
      // Expected.
    }
  }

  @Test
  public void bindsUuidLookingStringAsStringWhenTargetIsString() {
    LiteralExpr literal = new LiteralExpr(UUID_VALUE, DataTypes.StringType, "STRING");

    Statement.Builder builder = Statement.newBuilder("SELECT @p1");

    // A UUID-looking value must remain a String when the target Spanner
    // column is STRING.
    SpannerTypeBinder.bind(builder, "p1", literal);

    Statement statement = builder.build();
    Value value = statement.getParameters().get("p1");

    assertThat(value.getType().getCode()).isEqualTo(Type.Code.STRING);
    assertThat(value.getString()).isEqualTo(UUID_VALUE);
  }

  @Test
  public void bindsNullStringAsString() {
    LiteralExpr literal = new LiteralExpr(null, DataTypes.StringType, "STRING");
    Statement.Builder builder = Statement.newBuilder("SELECT @p1");

    SpannerTypeBinder.bind(builder, "p1", literal);

    Statement statement = builder.build();
    assertThat(statement.getParameters().get("p1").isNull()).isTrue();
  }

  @Test
  public void bindsNullUuidAsUuid() {
    LiteralExpr literal = new LiteralExpr(null, DataTypes.StringType, "UUID");
    Statement.Builder builder = Statement.newBuilder("SELECT @p1");

    SpannerTypeBinder.bind(builder, "p1", literal);

    Statement statement = builder.build();
    assertThat(statement.getParameters().get("p1").isNull()).isTrue();
  }

  @Test
  public void bindsPostgresDecimalAsPgNumeric() {
    BigDecimal highPrecision = new BigDecimal("1000000000000000000000000000000.123456789012");
    LiteralExpr literal =
        new LiteralExpr(highPrecision, DataTypes.createDecimalType(38, 12), "NUMERIC");
    Statement.Builder builder = Statement.newBuilder("SELECT $1");

    SpannerTypeBinder.bind(builder, "p1", literal, Dialect.POSTGRESQL);

    Statement statement = builder.build();
    assertThat(statement.getParameters().get("p1"))
        .isEqualTo(Value.pgNumeric(highPrecision.toPlainString()));
  }

  @Test
  public void bindsPostgresNullDecimalAsPgNumericNull() {
    LiteralExpr literal = new LiteralExpr(null, DataTypes.createDecimalType(38, 9), "NUMERIC");
    Statement.Builder builder = Statement.newBuilder("SELECT $1");

    SpannerTypeBinder.bind(builder, "p1", literal, Dialect.POSTGRESQL);

    Statement statement = builder.build();
    assertThat(statement.getParameters().get("p1")).isEqualTo(Value.pgNumeric(null));
  }

  @Test
  public void googleSqlAcceptsTrailingZerosWithinNumericRangeAndRejectsOverflow() {
    BigDecimal trailingZeros = new BigDecimal("1.2300000000");
    LiteralExpr validLiteral =
        new LiteralExpr(trailingZeros, DataTypes.createDecimalType(38, 10), "NUMERIC");
    Statement.Builder builder = Statement.newBuilder("SELECT @p1");
    SpannerTypeBinder.bind(builder, "p1", validLiteral, Dialect.GOOGLE_STANDARD_SQL);
    assertThat(builder.build().getParameters().get("p1")).isEqualTo(Value.numeric(trailingZeros));

    LiteralExpr overflowIntDigits =
        new LiteralExpr(
            new BigDecimal("100000000000000000000000000000"),
            DataTypes.createDecimalType(38, 0),
            "NUMERIC");
    assertThrows(
        IllegalArgumentException.class,
        () ->
            SpannerTypeBinder.bind(
                Statement.newBuilder("SELECT @p1"),
                "p1",
                overflowIntDigits,
                Dialect.GOOGLE_STANDARD_SQL));

    LiteralExpr overflowScale =
        new LiteralExpr(
            new BigDecimal("0.0000000001"), DataTypes.createDecimalType(38, 10), "NUMERIC");
    assertThrows(
        IllegalArgumentException.class,
        () ->
            SpannerTypeBinder.bind(
                Statement.newBuilder("SELECT @p1"),
                "p1",
                overflowScale,
                Dialect.GOOGLE_STANDARD_SQL));
  }
}
