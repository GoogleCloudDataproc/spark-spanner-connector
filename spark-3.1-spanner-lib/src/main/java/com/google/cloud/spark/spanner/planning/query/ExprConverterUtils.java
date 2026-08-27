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

import com.google.cloud.spark.spanner.SpannerConnectorException;
import com.google.cloud.spark.spanner.SpannerErrorCode;
import com.google.cloud.spark.spanner.planning.expression.*;
import java.time.LocalDate;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExprConverterUtils {
  private static final Logger logger = LoggerFactory.getLogger(ExprConverterUtils.class);

  public static ColumnExpr toColumn(String name, Map<String, ColumnResolution> resolutionMap) {
    logger.debug("Looking up column '{}' in resolutionMap {}", name, resolutionMap);

    ColumnResolution columnResolution = resolutionMap.get(name);

    return new ColumnExpr(
        columnResolution.getTableAlias(),
        columnResolution.getColumnName(),
        columnResolution.getSparkType(),
        columnResolution.isNullable());
  }

  public static LiteralExpr toLiteral(
      Object value, Map<String, ColumnResolution> resolutionMap, String columnName) {
    logger.debug("Looking up literal column '{}' in resolutionMap {}", columnName, resolutionMap);

    ColumnResolution columnResolution = resolutionMap.get(columnName);

    if (columnResolution == null) {
      throw new IllegalArgumentException("No column resolution found for column: " + columnName);
    }

    return toLiteral(value, columnResolution);
  }

  public static LiteralExpr toLiteral(Object value, ColumnResolution columnResolution) {

    Objects.requireNonNull(columnResolution, "columnResolution cannot be null");

    Object normalizedValue = normalizeLiteral(value, columnResolution.getSparkType());

    if (ColumnResolution.isUuid(columnResolution.getSpannerType()) && normalizedValue != null) {
      validateUuid(normalizedValue);
    }

    return new LiteralExpr(
        normalizedValue, columnResolution.getSparkType(), columnResolution.getSpannerType());
  }

  private static void validateUuid(Object value) {
    try {
      UUID.fromString(value.toString());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid UUID value for Spanner UUID column: " + value, e);
    }
  }

  public static Object normalizeLiteral(Object value, DataType type) {
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

      throw new SpannerConnectorException(
          SpannerErrorCode.UNSUPPORTED_DATATYPE,
          "Unexpected DateType literal value: " + value.getClass());
    }

    if (type instanceof DecimalType) {
      return ((Decimal) value).toJavaBigDecimal();
    }

    return value;
  }
}
