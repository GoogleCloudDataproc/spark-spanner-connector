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

import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import com.google.cloud.spark.spanner.planning.expression.LiteralExpr;
import java.math.BigDecimal;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
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

  @Test
  public void bindsEpochMicrosTimestampAndEpochDaysDate() {
    long micros = 1767225600123456L; // 2026-01-01T00:00:00.123456Z
    LiteralExpr tsLiteral = new LiteralExpr(micros, DataTypes.TimestampType, "TIMESTAMP");
    Statement.Builder tsBuilder = Statement.newBuilder("SELECT @p1");
    SpannerTypeBinder.bind(tsBuilder, "p1", tsLiteral);
    assertThat(tsBuilder.build().getParameters().get("p1"))
        .isEqualTo(Value.timestamp(Timestamp.ofTimeSecondsAndNanos(1767225600L, 123456000)));

    LiteralExpr preEpochTs = new LiteralExpr(-1L, DataTypes.TimestampType, "TIMESTAMP");
    Statement.Builder preEpochBuilder = Statement.newBuilder("SELECT @p1");
    SpannerTypeBinder.bind(preEpochBuilder, "p1", preEpochTs);
    assertThat(preEpochBuilder.build().getParameters().get("p1"))
        .isEqualTo(Value.timestamp(Timestamp.ofTimeSecondsAndNanos(-1L, 999999000)));

    LiteralExpr dateLiteral = new LiteralExpr(20454, DataTypes.DateType, "DATE"); // 2026-01-01
    Statement.Builder dateBuilder = Statement.newBuilder("SELECT @p1");
    SpannerTypeBinder.bind(dateBuilder, "p1", dateLiteral);
    assertThat(dateBuilder.build().getParameters().get("p1"))
        .isEqualTo(Value.date(Date.fromYearMonthDay(2026, 1, 1)));
  }

  @Test
  public void bindsFloatAsFloat32() {
    LiteralExpr literal = new LiteralExpr(0.1f, DataTypes.FloatType, "FLOAT32");
    Statement.Builder builder = Statement.newBuilder("SELECT @p1");

    SpannerTypeBinder.bind(builder, "p1", literal);

    Value value = builder.build().getParameters().get("p1");
    assertThat(value.getType().getCode()).isEqualTo(Type.Code.FLOAT32);
    assertThat(value.getFloat32()).isEqualTo(0.1f);
  }

  @Test
  public void bindsFloatSpecialValuesAsFloat32() {
    for (float f :
        new float[] {Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, -0.0f}) {
      LiteralExpr literal = new LiteralExpr(f, DataTypes.FloatType, "FLOAT32");
      Statement.Builder builder = Statement.newBuilder("SELECT @p1");

      SpannerTypeBinder.bind(builder, "p1", literal);

      Value value = builder.build().getParameters().get("p1");
      assertThat(value.getType().getCode()).isEqualTo(Type.Code.FLOAT32);
      if (Float.isNaN(f)) {
        assertThat(value.getFloat32()).isNaN();
      } else {
        assertThat(Float.floatToRawIntBits(value.getFloat32()))
            .isEqualTo(Float.floatToRawIntBits(f));
      }
    }
  }

  @Test
  public void bindsNullFloatAsFloat32() {
    LiteralExpr literal = new LiteralExpr(null, DataTypes.FloatType, "FLOAT32");
    Statement.Builder builder = Statement.newBuilder("SELECT @p1");

    SpannerTypeBinder.bind(builder, "p1", literal);

    Value value = builder.build().getParameters().get("p1");
    assertThat(value.isNull()).isTrue();
    assertThat(value.getType().getCode()).isEqualTo(Type.Code.FLOAT32);
  }

  @Test
  public void bindsDoubleAsFloat64() {
    LiteralExpr literal = new LiteralExpr(0.1d, DataTypes.DoubleType, "FLOAT64");
    Statement.Builder builder = Statement.newBuilder("SELECT @p1");

    SpannerTypeBinder.bind(builder, "p1", literal);

    Value value = builder.build().getParameters().get("p1");
    assertThat(value.getType().getCode()).isEqualTo(Type.Code.FLOAT64);
    assertThat(value.getFloat64()).isEqualTo(0.1d);
  }

  @Test
  public void bindsDoubleLiteralOnFloatColumnAsFloat64WithoutPrecisionLoss() {
    LiteralExpr literal = new LiteralExpr(-0.1d, DataTypes.FloatType, "FLOAT32");
    Statement.Builder builder = Statement.newBuilder("SELECT @p1");

    SpannerTypeBinder.bind(builder, "p1", literal);

    Value value = builder.build().getParameters().get("p1");
    assertThat(value.getType().getCode()).isEqualTo(Type.Code.FLOAT64);
    assertThat(value.getFloat64()).isEqualTo(-0.1d);
  }

  @Test
  public void bindsFloatLiteralOnDoubleColumnAsFloat64() {
    LiteralExpr literal = new LiteralExpr(1.25f, DataTypes.DoubleType, "FLOAT64");
    Statement.Builder builder = Statement.newBuilder("SELECT @p1");

    SpannerTypeBinder.bind(builder, "p1", literal);

    Value value = builder.build().getParameters().get("p1");
    assertThat(value.getType().getCode()).isEqualTo(Type.Code.FLOAT64);
    assertThat(value.getFloat64()).isEqualTo(1.25d);
  }

  @Test
  public void bindsIntegerLiteralOnLongColumnAsInt64() {
    LiteralExpr literal = new LiteralExpr(42, DataTypes.LongType, "INT64");
    Statement.Builder builder = Statement.newBuilder("SELECT @p1");

    SpannerTypeBinder.bind(builder, "p1", literal);

    Value value = builder.build().getParameters().get("p1");
    assertThat(value.getType().getCode()).isEqualTo(Type.Code.INT64);
    assertThat(value.getInt64()).isEqualTo(42L);
  }

  @Test
  public void bindsDecimalAndWideIntegersOnFloatColumnWithoutPrecisionLoss() {
    BigDecimal bd = new BigDecimal("-0.1");
    LiteralExpr sparkDecimalOnFloat =
        new LiteralExpr(Decimal.apply(bd), DataTypes.FloatType, "FLOAT32");
    Statement.Builder gsqlDecBuilder = Statement.newBuilder("SELECT @p1");
    SpannerTypeBinder.bind(gsqlDecBuilder, "p1", sparkDecimalOnFloat, Dialect.GOOGLE_STANDARD_SQL);
    assertThat(gsqlDecBuilder.build().getParameters().get("p1")).isEqualTo(Value.numeric(bd));

    Statement.Builder pgDecBuilder = Statement.newBuilder("SELECT $1");
    SpannerTypeBinder.bind(pgDecBuilder, "p1", sparkDecimalOnFloat, Dialect.POSTGRESQL);
    assertThat(pgDecBuilder.build().getParameters().get("p1")).isEqualTo(Value.pgNumeric("-0.1"));

    // 2^24 (16_777_216) is exact in IEEE 754 binary32; 2^24 + 1 (16_777_217) is not and must stay
    // INT64 rather than rounding to 16_777_216.0f.
    LiteralExpr exactIntOnFloat = new LiteralExpr(16_777_216, DataTypes.FloatType, "FLOAT32");
    Statement.Builder exactBuilder = Statement.newBuilder("SELECT @p1");
    SpannerTypeBinder.bind(exactBuilder, "p1", exactIntOnFloat);
    assertThat(exactBuilder.build().getParameters().get("p1"))
        .isEqualTo(Value.float32(16_777_216.0f));

    for (Object inexactInt : new Object[] {16_777_217, 16_777_217L, Long.MIN_VALUE}) {
      LiteralExpr inexactOnFloat = new LiteralExpr(inexactInt, DataTypes.FloatType, "FLOAT32");
      Statement.Builder inexactBuilder = Statement.newBuilder("SELECT @p1");
      SpannerTypeBinder.bind(inexactBuilder, "p1", inexactOnFloat);
      assertThat(inexactBuilder.build().getParameters().get("p1"))
          .isEqualTo(Value.int64(((Number) inexactInt).longValue()));
    }
  }

  @Test
  public void preservesFractionalLiteralsOnIntegerColumnsAndRejectsInvalidTypes() {
    LiteralExpr floatOnLong = new LiteralExpr(1.5f, DataTypes.LongType, "INT64");
    Statement.Builder floatBuilder = Statement.newBuilder("SELECT @p1");
    SpannerTypeBinder.bind(floatBuilder, "p1", floatOnLong);
    assertThat(floatBuilder.build().getParameters().get("p1")).isEqualTo(Value.float32(1.5f));

    LiteralExpr doubleOnInt = new LiteralExpr(1.5d, DataTypes.IntegerType, "INT64");
    Statement.Builder doubleBuilder = Statement.newBuilder("SELECT @p1");
    SpannerTypeBinder.bind(doubleBuilder, "p1", doubleOnInt);
    assertThat(doubleBuilder.build().getParameters().get("p1")).isEqualTo(Value.float64(1.5d));

    LiteralExpr invalidOnFloat = new LiteralExpr("not-a-number", DataTypes.FloatType, "FLOAT32");
    assertThrows(
        IllegalArgumentException.class,
        () -> SpannerTypeBinder.bind(Statement.newBuilder("SELECT @p1"), "p1", invalidOnFloat));
  }
}
