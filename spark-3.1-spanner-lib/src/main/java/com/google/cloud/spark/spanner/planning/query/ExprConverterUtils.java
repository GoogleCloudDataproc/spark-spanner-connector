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

import com.google.cloud.spark.spanner.binding.SpannerTypeBinder;
import com.google.cloud.spark.spanner.planning.expression.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExprConverterUtils {
  private static final Logger logger = LoggerFactory.getLogger(ExprConverterUtils.class);

  public static ColumnExpr toColumn(String name, Map<String, ColumnResolution> resolutionMap) {
    logger.debug("Looking up column '{}' in resolutionMap {}", name, resolutionMap);

    if (resolutionMap == null) {
      throw new IllegalArgumentException("No column resolution found for column: " + name);
    }

    ColumnResolution columnResolution = resolutionMap.get(name);
    if (columnResolution == null) {
      throw new IllegalArgumentException("No column resolution found for column: " + name);
    }

    validateSupportedColumn(columnResolution);

    return new ColumnExpr(
        columnResolution.getTableAlias(),
        columnResolution.getColumnName(),
        columnResolution.getSparkType(),
        columnResolution.isNullable());
  }

  public static LiteralExpr toLiteral(
      Object value, Map<String, ColumnResolution> resolutionMap, String columnName) {
    return toLiteral(value, resolutionMap, columnName, false);
  }

  public static LiteralExpr toLiteral(
      Object value,
      Map<String, ColumnResolution> resolutionMap,
      String columnName,
      boolean isPostgreSql) {
    logger.debug("Looking up literal column '{}' in resolutionMap {}", columnName, resolutionMap);

    if (resolutionMap == null) {
      throw new IllegalArgumentException("No column resolution found for column: " + columnName);
    }

    ColumnResolution columnResolution = resolutionMap.get(columnName);

    if (columnResolution == null) {
      throw new IllegalArgumentException("No column resolution found for column: " + columnName);
    }

    return toLiteral(value, columnResolution, isPostgreSql);
  }

  public static LiteralExpr toLiteral(Object value, ColumnResolution columnResolution) {
    return toLiteral(value, columnResolution, false);
  }

  public static LiteralExpr toLiteral(
      Object value, ColumnResolution columnResolution, boolean isPostgreSql) {

    Objects.requireNonNull(columnResolution, "columnResolution cannot be null");
    validateSupportedColumn(columnResolution);

    Object normalizedValue = normalizeLiteral(value, columnResolution.getSparkType());

    if (ColumnResolution.isUuid(columnResolution.getSpannerType()) && normalizedValue != null) {
      validateUuid(normalizedValue);
    }

    if (!isPostgreSql
        && normalizedValue instanceof BigDecimal
        && !SpannerTypeBinder.isValidGoogleSqlNumeric((BigDecimal) normalizedValue)) {
      throw new IllegalArgumentException(
          "Decimal out of Spanner NUMERIC range: " + normalizedValue);
    }

    return new LiteralExpr(
        normalizedValue, columnResolution.getSparkType(), columnResolution.getSpannerType());
  }

  public static void validateSupportedColumn(ColumnResolution columnResolution) {
    DataType sparkType = columnResolution.getSparkType();
    String spannerType = columnResolution.getSpannerType();
    if (sparkType == null
        || sparkType.sameType(DataTypes.NullType)
        || sparkType instanceof StructType
        || sparkType instanceof ArrayType
        || !SpannerTypeBinder.isSupportedSparkType(sparkType)) {
      throw new UnsupportedOperationException(
          "Unsupported Spark column type for predicate pushdown: " + sparkType);
    }
    if (ColumnResolution.isJson(spannerType) || ColumnResolution.isJsonb(spannerType)) {
      throw new UnsupportedOperationException(
          "Unsupported Spanner column type for predicate pushdown: " + spannerType);
    }
  }

  private static void validateUuid(Object value) {
    String str = value.toString();
    if (str.length() != 36) {
      throw new IllegalArgumentException("Invalid UUID value for Spanner UUID column: " + value);
    }
    try {
      if (!UUID.fromString(str).toString().equals(str)) {
        throw new IllegalArgumentException(
            "Non-canonical UUID value for Spanner UUID column: " + value);
      }
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid UUID value for Spanner UUID column: " + value, e);
    }
  }

  public static Object normalizeLiteral(Object value, DataType type) {
    if (!SpannerTypeBinder.isSupportedSparkType(type)) {
      throw new UnsupportedOperationException(
          "Unsupported literal type for predicate pushdown: " + type);
    }

    if (value == null) {
      return null;
    }

    if (type.sameType(DataTypes.StringType)) {
      return value.toString();
    }

    if (type.sameType(DataTypes.DateType)) {
      if (value instanceof LocalDate) {
        return value;
      }

      if (value instanceof java.sql.Date) {
        return ((java.sql.Date) value).toLocalDate();
      }

      if (value instanceof Number) {
        return LocalDate.ofEpochDay(((Number) value).longValue());
      }

      throw new IllegalArgumentException("Unexpected DateType literal value: " + value.getClass());
    }

    if (type.sameType(DataTypes.TimestampType)) {
      if (value instanceof Instant) {
        return value;
      }

      if (value instanceof java.sql.Timestamp) {
        return ((java.sql.Timestamp) value).toInstant();
      }

      if (value instanceof Number) {
        long micros = ((Number) value).longValue();
        return Instant.ofEpochSecond(
            Math.floorDiv(micros, 1_000_000L), Math.floorMod(micros, 1_000_000L) * 1_000L);
      }

      throw new IllegalArgumentException(
          "Unexpected TimestampType literal value: " + value.getClass());
    }

    if (type instanceof DecimalType) {
      if (value instanceof BigDecimal) {
        return value;
      }
      if (value instanceof Decimal) {
        return ((Decimal) value).toJavaBigDecimal();
      }
      throw new IllegalArgumentException(
          "Unexpected DecimalType literal value: " + value.getClass());
    }

    return value;
  }
}
