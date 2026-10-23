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

import static com.google.cloud.spark.spanner.planning.query.ColumnResolution.isUuid;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import com.google.cloud.spark.spanner.planning.expression.LiteralExpr;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;

public final class SpannerTypeBinder {

  public static boolean isValidGoogleSqlNumeric(BigDecimal bd) {
    BigDecimal stripped = bd.stripTrailingZeros();
    return stripped.scale() <= 9 && (stripped.precision() - stripped.scale()) <= 29;
  }

  public static boolean isSupportedSparkType(DataType type) {
    if (type == null) {
      return false;
    }
    return type instanceof DecimalType
        || type.sameType(DataTypes.StringType)
        || type.sameType(DataTypes.LongType)
        || type.sameType(DataTypes.IntegerType)
        || type.sameType(DataTypes.ShortType)
        || type.sameType(DataTypes.ByteType)
        || type.sameType(DataTypes.BooleanType)
        || type.sameType(DataTypes.DoubleType)
        || type.sameType(DataTypes.FloatType)
        || type.sameType(DataTypes.TimestampType)
        || type.sameType(DataTypes.DateType)
        || type.sameType(DataTypes.BinaryType);
  }

  public static void bind(Statement.Builder builder, String parameter, LiteralExpr literal) {
    bind(builder, parameter, literal, Dialect.GOOGLE_STANDARD_SQL);
  }

  public static void bind(
      Statement.Builder builder, String parameter, LiteralExpr literal, Dialect dialect) {

    Object value = literal.getValue();
    DataType sparkType = literal.getSparkType();
    String spannerType = literal.getSpannerType();

    if (value == null) {
      bindNull(builder, parameter, sparkType, spannerType, dialect);
      return;
    }

    if (isUuid(spannerType)) {
      bindUuid(builder, parameter, value);
      return;
    }

    if (sparkType instanceof DecimalType) {
      bindDecimal(builder, parameter, value, dialect);
    } else if (sparkType.sameType(DataTypes.StringType)) {
      builder.bind(parameter).to(value.toString());
    } else if (sparkType.sameType(DataTypes.LongType)
        || sparkType.sameType(DataTypes.IntegerType)
        || sparkType.sameType(DataTypes.ShortType)
        || sparkType.sameType(DataTypes.ByteType)) {
      if (value instanceof Long
          || value instanceof Integer
          || value instanceof Short
          || value instanceof Byte) {
        builder.bind(parameter).to(((Number) value).longValue());
      } else if (value instanceof Float) {
        builder.bind(parameter).to((Float) value);
      } else if (value instanceof Double) {
        builder.bind(parameter).to((Double) value);
      } else if (value instanceof Decimal || value instanceof BigDecimal) {
        bindDecimal(builder, parameter, value, dialect);
      } else {
        throw new IllegalArgumentException("Unexpected integer literal type: " + value.getClass());
      }
    } else if (sparkType.sameType(DataTypes.BooleanType)) {
      if (!(value instanceof Boolean)) {
        throw new IllegalArgumentException("Unexpected boolean literal type: " + value.getClass());
      }
      builder.bind(parameter).to((Boolean) value);
    } else if (sparkType.sameType(DataTypes.DoubleType)) {
      if (value instanceof Decimal || value instanceof BigDecimal) {
        bindDecimal(builder, parameter, value, dialect);
      } else if (value instanceof Long
          && ((Long) value < -(1L << 53) || (Long) value > (1L << 53))) {
        builder.bind(parameter).to((Long) value);
      } else if (value instanceof Number) {
        builder.bind(parameter).to(((Number) value).doubleValue());
      } else {
        throw new IllegalArgumentException("Unexpected double literal type: " + value.getClass());
      }
    } else if (sparkType.sameType(DataTypes.FloatType)) {
      // Spark FloatType columns map to Spanner FLOAT32 columns. Preserve wider numeric literals
      // (Double, BigDecimal/Decimal, and Long/Integer outside the exact 24-bit float32 integer
      // range) so mixed-precision predicates (e.g., float32_col = -0.1d or 16777217L) are not
      // lossily narrowed.
      if (value instanceof Float || value instanceof Short || value instanceof Byte) {
        builder.bind(parameter).to(((Number) value).floatValue());
      } else if (value instanceof Double) {
        builder.bind(parameter).to((Double) value);
      } else if (value instanceof Decimal || value instanceof BigDecimal) {
        bindDecimal(builder, parameter, value, dialect);
      } else if (value instanceof Long || value instanceof Integer) {
        long lv = ((Number) value).longValue();
        if (lv >= -(1L << 24) && lv <= (1L << 24)) {
          builder.bind(parameter).to((float) lv);
        } else {
          builder.bind(parameter).to(lv);
        }
      } else {
        throw new IllegalArgumentException("Unexpected float literal type: " + value.getClass());
      }
    } else if (sparkType.sameType(DataTypes.TimestampType)) {
      Instant instant;
      if (value instanceof Instant) {
        instant = (Instant) value;
      } else if (value instanceof java.sql.Timestamp) {
        instant = ((java.sql.Timestamp) value).toInstant();
      } else if (value instanceof Number) {
        long micros = ((Number) value).longValue();
        instant =
            Instant.ofEpochSecond(
                Math.floorDiv(micros, 1_000_000L), Math.floorMod(micros, 1_000_000L) * 1_000L);
      } else {
        throw new IllegalArgumentException(
            "Unexpected timestamp literal type: " + value.getClass());
      }
      builder
          .bind(parameter)
          .to(
              com.google.cloud.Timestamp.ofTimeSecondsAndNanos(
                  instant.getEpochSecond(), instant.getNano()));
    } else if (sparkType.sameType(DataTypes.DateType)) {
      LocalDate localDate;
      if (value instanceof LocalDate) {
        localDate = (LocalDate) value;
      } else if (value instanceof java.sql.Date) {
        localDate = ((java.sql.Date) value).toLocalDate();
      } else if (value instanceof Number) {
        localDate = LocalDate.ofEpochDay(((Number) value).longValue());
      } else {
        throw new IllegalArgumentException("Unexpected date literal type: " + value.getClass());
      }
      builder
          .bind(parameter)
          .to(
              com.google.cloud.Date.fromYearMonthDay(
                  localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth()));
    } else if (sparkType.sameType(DataTypes.BinaryType)) {
      byte[] bytes;
      if (value instanceof byte[]) {
        bytes = (byte[]) value;
      } else if (value instanceof Byte[]) {
        Byte[] boxed = (Byte[]) value;
        bytes = new byte[boxed.length];
        for (int i = 0; i < boxed.length; i++) {
          bytes[i] = boxed[i];
        }
      } else {
        throw new IllegalArgumentException("Unexpected binary literal type: " + value.getClass());
      }
      builder.bind(parameter).to(ByteArray.copyFrom(bytes));
    } else {
      throw new UnsupportedOperationException("Unsupported type: " + sparkType);
    }
  }

  private static Type mapToSpannerType(DataType type) {
    if (type instanceof DecimalType) {
      return Type.numeric();
    } else if (type.sameType(DataTypes.StringType)) {
      return Type.string();
    } else if (type.sameType(DataTypes.LongType)
        || type.sameType(DataTypes.IntegerType)
        || type.sameType(DataTypes.ShortType)
        || type.sameType(DataTypes.ByteType)) {
      return Type.int64();
    } else if (type.sameType(DataTypes.BooleanType)) {
      return Type.bool();
    } else if (type.sameType(DataTypes.DoubleType)) {
      return Type.float64();
    } else if (type.sameType(DataTypes.FloatType)) {
      return Type.float32();
    } else if (type.sameType(DataTypes.TimestampType)) {
      return Type.timestamp();
    } else if (type.sameType(DataTypes.DateType)) {
      return Type.date();
    } else if (type.sameType(DataTypes.BinaryType)) {
      return Type.bytes();
    } else {
      throw new UnsupportedOperationException(
          "Unsupported Spark type for Spanner null mapping: " + type);
    }
  }

  private static void bindNull(
      Statement.Builder builder,
      String parameter,
      DataType sparkType,
      String spannerType,
      Dialect dialect) {
    if (isUuid(spannerType)) {
      builder.bind(parameter).to((UUID) null);
      return;
    }

    Type mappedType = mapToSpannerType(sparkType);

    switch (mappedType.getCode()) {
      case STRING:
        builder.bind(parameter).to((String) null);
        break;

      case INT64:
        builder.bind(parameter).to((Long) null);
        break;

      case BOOL:
        builder.bind(parameter).to((Boolean) null);
        break;

      case FLOAT64:
        builder.bind(parameter).to((Double) null);
        break;

      case FLOAT32:
        builder.bind(parameter).to((Float) null);
        break;

      case NUMERIC:
        if (dialect == Dialect.POSTGRESQL) {
          builder.bind(parameter).to(Value.pgNumeric(null));
        } else {
          builder.bind(parameter).to((BigDecimal) null);
        }
        break;

      case TIMESTAMP:
        builder.bind(parameter).to((com.google.cloud.Timestamp) null);
        break;

      case DATE:
        builder.bind(parameter).to((com.google.cloud.Date) null);
        break;

      case BYTES:
        builder.bind(parameter).to((ByteArray) null);
        break;

      default:
        throw new UnsupportedOperationException(
            "Unsupported Spanner type for null binding: " + mappedType);
    }
  }

  private static void bindDecimal(
      Statement.Builder builder, String parameter, Object value, Dialect dialect) {
    BigDecimal bd;
    if (value instanceof Decimal) {
      bd = ((Decimal) value).toJavaBigDecimal();
    } else if (value instanceof BigDecimal) {
      bd = (BigDecimal) value;
    } else {
      throw new IllegalArgumentException("Unexpected decimal literal type: " + value.getClass());
    }
    if (dialect == Dialect.POSTGRESQL) {
      builder.bind(parameter).to(Value.pgNumeric(bd.toPlainString()));
    } else if (isValidGoogleSqlNumeric(bd)) {
      builder.bind(parameter).to(bd);
    } else {
      throw new IllegalArgumentException("Decimal out of Spanner NUMERIC range: " + bd);
    }
  }

  private static void bindUuid(Statement.Builder builder, String parameter, Object value) {

    if (!(value instanceof String)) {
      throw new IllegalArgumentException(
          "Expected String value for UUID parameter but got: " + value.getClass().getName());
    }

    UUID uuid;
    try {
      uuid = UUID.fromString((String) value);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid UUID value: " + value, e);
    }

    builder.bind(parameter).to(uuid);
  }
}
