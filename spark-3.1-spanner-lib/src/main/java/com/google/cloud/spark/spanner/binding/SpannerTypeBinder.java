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

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Type;
import com.google.cloud.spark.spanner.planning.expression.LiteralExpr;
import java.math.BigDecimal;
import java.time.Instant;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;

public final class SpannerTypeBinder {

  public static void bind(Statement.Builder builder, String parameter, LiteralExpr literal) {

    Object value = literal.getValue();
    DataType type = literal.getSparkType();

    if (value == null) {
      builder.bind(parameter).to(mapToSpannerType(type), (com.google.cloud.spanner.Struct) null);
      return;
    }

    if (type instanceof DecimalType) {
      BigDecimal bd = (BigDecimal) value;
      // Spanner NUMERIC validation: precision <= 38, scale <= 9
      if (bd.precision() <= 38 && bd.scale() <= 9) {
        builder.bind(parameter).to(bd);
      } else {
        throw new IllegalArgumentException("Decimal out of Spanner NUMERIC range");
      }
    } else if (type.sameType(DataTypes.StringType)) {
      builder.bind(parameter).to((String) value);
    } else if (type.sameType(DataTypes.LongType)) {
      builder.bind(parameter).to((Long) value);
    } else if (type.sameType(DataTypes.BooleanType)) {
      builder.bind(parameter).to((Boolean) value);
    } else if (type.sameType(DataTypes.IntegerType)) {
      builder.bind(parameter).to(((Integer) value).longValue());
    } else if (type.sameType(DataTypes.ShortType)) {
      builder.bind(parameter).to(((Short) value).longValue());
    } else if (type.sameType(DataTypes.ByteType)) {
      builder.bind(parameter).to(((Byte) value).longValue());
    } else if (type.sameType(DataTypes.DoubleType)) {
      builder.bind(parameter).to((Double) value);
    } else if (type.sameType(DataTypes.FloatType)) {
      builder.bind(parameter).to(((Float) value).doubleValue());
    } else if (type.sameType(DataTypes.TimestampType)) {

      if (value instanceof Instant) {
        Instant instant = (Instant) value;
        builder
            .bind(parameter)
            .to(
                com.google.cloud.Timestamp.ofTimeSecondsAndNanos(
                    instant.getEpochSecond(), instant.getNano()));

      } else if (value instanceof java.sql.Timestamp) {
        java.sql.Timestamp ts = (java.sql.Timestamp) value;
        Instant instant = ts.toInstant();
        builder
            .bind(parameter)
            .to(
                com.google.cloud.Timestamp.ofTimeSecondsAndNanos(
                    instant.getEpochSecond(), instant.getNano()));

      } else {
        throw new IllegalArgumentException(
            "Unexpected timestamp literal type: " + value.getClass());
      }
    } else if (type.sameType(DataTypes.DateType)) {
      java.sql.Date date = (java.sql.Date) value;

      builder
          .bind(parameter)
          .to(
              com.google.cloud.Date.fromYearMonthDay(
                  date.toLocalDate().getYear(),
                  date.toLocalDate().getMonthValue(),
                  date.toLocalDate().getDayOfMonth()));
    } else if (type.sameType(DataTypes.BinaryType)) {
      builder.bind(parameter).to(ByteArray.copyFrom((byte[]) value));
    } else {
      throw new UnsupportedOperationException("Unsupported type: " + type);
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
    } else if (type.sameType(DataTypes.DoubleType) || type.sameType(DataTypes.FloatType)) {
      return Type.float64();
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
}
