// Copyright 2023 Google LLC
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

package com.google.cloud.spark.spanner;

import static java.lang.String.format;

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SparkFilterUtils {
  private SparkFilterUtils() {}

  // Structs are not handled
  public static boolean isTopLevelFieldHandled(
      boolean pushAllFilters, Filter filter, Map<String, StructField> fields) {
    if (pushAllFilters) {
      return true;
    }
    if (filter instanceof EqualTo) {
      EqualTo equalTo = (EqualTo) filter;
      return isFilterWithNamedFieldHandled(pushAllFilters, filter, fields, equalTo.attribute());
    }
    if (filter instanceof EqualNullSafe) {
      EqualNullSafe equalNullSafe = (EqualNullSafe) filter;
      return isFilterWithNamedFieldHandled(
          pushAllFilters, filter, fields, equalNullSafe.attribute());
    }
    if (filter instanceof GreaterThan) {
      GreaterThan greaterThan = (GreaterThan) filter;
      return isFilterWithNamedFieldHandled(pushAllFilters, filter, fields, greaterThan.attribute());
    }
    if (filter instanceof GreaterThanOrEqual) {
      GreaterThanOrEqual greaterThanOrEqual = (GreaterThanOrEqual) filter;
      return isFilterWithNamedFieldHandled(
          pushAllFilters, filter, fields, greaterThanOrEqual.attribute());
    }
    if (filter instanceof LessThan) {
      LessThan lessThan = (LessThan) filter;
      return isFilterWithNamedFieldHandled(pushAllFilters, filter, fields, lessThan.attribute());
    }
    if (filter instanceof LessThanOrEqual) {
      LessThanOrEqual lessThanOrEqual = (LessThanOrEqual) filter;
      return isFilterWithNamedFieldHandled(
          pushAllFilters, filter, fields, lessThanOrEqual.attribute());
    }
    if (filter instanceof In) {
      In in = (In) filter;
      return isFilterWithNamedFieldHandled(pushAllFilters, filter, fields, in.attribute());
    }
    if (filter instanceof IsNull) {
      IsNull isNull = (IsNull) filter;
      return isFilterWithNamedFieldHandled(pushAllFilters, filter, fields, isNull.attribute());
    }
    if (filter instanceof IsNotNull) {
      IsNotNull isNotNull = (IsNotNull) filter;
      return isFilterWithNamedFieldHandled(pushAllFilters, filter, fields, isNotNull.attribute());
    }
    if (filter instanceof And) {
      And and = (And) filter;
      return isTopLevelFieldHandled(pushAllFilters, and.left(), fields)
          && isTopLevelFieldHandled(pushAllFilters, and.right(), fields);
    }
    if (filter instanceof Or) {
      Or or = (Or) filter;
      return isTopLevelFieldHandled(pushAllFilters, or.left(), fields)
          && isTopLevelFieldHandled(pushAllFilters, or.right(), fields);
    }
    if (filter instanceof Not) {
      Not not = (Not) filter;
      return isTopLevelFieldHandled(pushAllFilters, not.child(), fields);
    }
    if (filter instanceof StringStartsWith) {
      StringStartsWith stringStartsWith = (StringStartsWith) filter;
      return isFilterWithNamedFieldHandled(
          pushAllFilters, filter, fields, stringStartsWith.attribute());
    }
    if (filter instanceof StringEndsWith) {
      StringEndsWith stringEndsWith = (StringEndsWith) filter;
      return isFilterWithNamedFieldHandled(
          pushAllFilters, filter, fields, stringEndsWith.attribute());
    }
    if (filter instanceof StringContains) {
      StringContains stringContains = (StringContains) filter;
      return isFilterWithNamedFieldHandled(
          pushAllFilters, filter, fields, stringContains.attribute());
    }

    throw new IllegalArgumentException(format("Invalid filter: %s", filter));
  }

  static boolean isFilterWithNamedFieldHandled(
      boolean pushAllFilters, Filter filter, Map<String, StructField> fields, String fieldName) {
    // For the Jsonb type in PostgreSql, the in filter will be translated to the
    // format CAST("jsoncol" AS VARCHAR) in ('', 'tags'), which is not allowed in
    // the Spanner.
    return Optional.ofNullable(fields.get(fieldName))
        .filter(
            field ->
                ((field.dataType() instanceof StructType)
                    || (field.dataType() instanceof ArrayType)
                    || ((filter instanceof In) && isJsonb(field))))
        .map(field -> false)
        .orElse(isHandled(pushAllFilters, filter));
  }

  public static boolean isHandled(boolean pushAllFilters, Filter filter) {
    if (pushAllFilters) {
      return true;
    }
    if (filter instanceof EqualTo
        || filter instanceof GreaterThan
        || filter instanceof GreaterThanOrEqual
        || filter instanceof LessThan
        || filter instanceof LessThanOrEqual
        || filter instanceof In
        || filter instanceof IsNull
        || filter instanceof IsNotNull
        || filter instanceof StringStartsWith
        || filter instanceof StringEndsWith
        || filter instanceof StringContains
        || filter instanceof EqualNullSafe) {
      return true;
    }
    if (filter instanceof And) {
      And and = (And) filter;
      return isHandled(pushAllFilters, and.left()) && isHandled(pushAllFilters, and.right());
    }
    if (filter instanceof Or) {
      Or or = (Or) filter;
      return isHandled(pushAllFilters, or.left()) && isHandled(pushAllFilters, or.right());
    }
    if (filter instanceof Not) {
      return isHandled(pushAllFilters, ((Not) filter).child());
    }
    return false;
  }

  public static Iterable<Filter> handledFilters(boolean pushAllFilters, Filter... filters) {
    return handledFilters(pushAllFilters, ImmutableList.copyOf(filters));
  }

  public static Iterable<Filter> handledFilters(boolean pushAllFilters, Iterable<Filter> filters) {
    return StreamSupport.stream(filters.spliterator(), false)
        .filter(f -> isHandled(pushAllFilters, f))
        .collect(Collectors.toList());
  }

  public static Iterable<Filter> unhandledFilters(boolean pushAllFilters, Filter... filters) {
    return unhandledFilters(pushAllFilters, ImmutableList.copyOf(filters));
  }

  public static Iterable<Filter> unhandledFilters(
      boolean pushAllFilters, Iterable<Filter> filters) {
    return StreamSupport.stream(filters.spliterator(), false)
        .filter(f -> !isHandled(pushAllFilters, f))
        .collect(Collectors.toList());
  }

  public static String getCompiledFilter(
      boolean pushAllFilters,
      Optional<String> configFilter,
      boolean isPostgreSql,
      Map<String, StructField> fields,
      Filter... pushedFilters) {
    String compiledPushedFilter =
        compileFilters(
            handledFilters(pushAllFilters, ImmutableList.copyOf(pushedFilters)),
            isPostgreSql,
            fields);
    return Stream.of(
            configFilter,
            compiledPushedFilter.length() == 0
                ? Optional.empty()
                : Optional.of(compiledPushedFilter))
        .filter(Optional::isPresent)
        .map(filter -> "(" + filter.get() + ")")
        .collect(Collectors.joining(" AND "));
  }

  // Mostly copied from JDBCRDD.scala
  public static String compileFilter(
      Filter filter, boolean isPostgreSql, Map<String, StructField> fields) {
    if (filter instanceof EqualTo) {
      EqualTo equalTo = (EqualTo) filter;
      return format(
          "%s = %s",
          quote(equalTo.attribute(), isPostgreSql, fields),
          compileValue(equalTo.value(), isPostgreSql));
    }
    if (filter instanceof EqualNullSafe) {
      EqualNullSafe equalNullSafe = (EqualNullSafe) filter;
      String leftNullCheck =
          quote(equalNullSafe.attribute(), isPostgreSql, fields, /* isNullCheck= */ true);
      String leftValueCheck = quote(equalNullSafe.attribute(), isPostgreSql, fields);
      String right = compileValue(equalNullSafe.value(), isPostgreSql);
      return format(
          "%1$s IS NULL AND %2$s IS NULL OR %1$s IS NOT NULL AND %2$s IS NOT NULL AND %3$s = %2$s",
          leftNullCheck, right, leftValueCheck);
    }
    if (filter instanceof GreaterThan) {
      GreaterThan greaterThan = (GreaterThan) filter;
      return format(
          "%s > %s",
          quote(greaterThan.attribute(), isPostgreSql, fields),
          compileValue(greaterThan.value(), isPostgreSql));
    }
    if (filter instanceof GreaterThanOrEqual) {
      GreaterThanOrEqual greaterThanOrEqual = (GreaterThanOrEqual) filter;
      return format(
          "%s >= %s",
          quote(greaterThanOrEqual.attribute(), isPostgreSql, fields),
          compileValue(greaterThanOrEqual.value(), isPostgreSql));
    }
    if (filter instanceof LessThan) {
      LessThan lessThan = (LessThan) filter;
      return format(
          "%s < %s",
          quote(lessThan.attribute(), isPostgreSql, fields),
          compileValue(lessThan.value(), isPostgreSql));
    }
    if (filter instanceof LessThanOrEqual) {
      LessThanOrEqual lessThanOrEqual = (LessThanOrEqual) filter;
      return format(
          "%s <= %s",
          quote(lessThanOrEqual.attribute(), isPostgreSql, fields),
          compileValue(lessThanOrEqual.value(), isPostgreSql));
    }
    if (filter instanceof In) {
      In in = (In) filter;
      return format(
          "%s IN %s",
          quote(in.attribute(), isPostgreSql, fields),
          compileValue(in.values(), /*arrayStart=*/ '(', /*arrayEnd=*/ ')', isPostgreSql));
    }
    if (filter instanceof IsNull) {
      IsNull isNull = (IsNull) filter;
      return format(
          "%s IS NULL", quote(isNull.attribute(), isPostgreSql, fields, /* isNullCheck= */ true));
    }
    if (filter instanceof IsNotNull) {
      IsNotNull isNotNull = (IsNotNull) filter;
      return format(
          "%s IS NOT NULL",
          quote(isNotNull.attribute(), isPostgreSql, fields, /* isNullCheck= */ true));
    }
    if (filter instanceof And) {
      And and = (And) filter;
      return format(
          "((%s) AND (%s))",
          compileFilter(and.left(), isPostgreSql, fields),
          compileFilter(and.right(), isPostgreSql, fields));
    }
    if (filter instanceof Or) {
      Or or = (Or) filter;
      return format(
          "((%s) OR (%s))",
          compileFilter(or.left(), isPostgreSql, fields),
          compileFilter(or.right(), isPostgreSql, fields));
    }
    if (filter instanceof Not) {
      Not not = (Not) filter;
      return format("(NOT (%s))", compileFilter(not.child(), isPostgreSql, fields));
    }
    if (filter instanceof StringStartsWith) {
      StringStartsWith stringStartsWith = (StringStartsWith) filter;
      return format(
          "%s LIKE '%s%%'",
          quote(stringStartsWith.attribute(), isPostgreSql, fields),
          escape(stringStartsWith.value()));
    }
    if (filter instanceof StringEndsWith) {
      StringEndsWith stringEndsWith = (StringEndsWith) filter;
      return format(
          "%s LIKE '%%%s'",
          quote(stringEndsWith.attribute(), isPostgreSql, fields), escape(stringEndsWith.value()));
    }
    if (filter instanceof StringContains) {
      StringContains stringContains = (StringContains) filter;
      return format(
          "%s LIKE '%%%s%%'",
          quote(stringContains.attribute(), isPostgreSql, fields), escape(stringContains.value()));
    }

    throw new IllegalArgumentException(format("Invalid filter: %s", filter));
  }

  public static String compileFilters(
      Iterable<Filter> filters, boolean isPostgreSql, Map<String, StructField> fields) {
    return StreamSupport.stream(filters.spliterator(), false)
        .map(filter -> SparkFilterUtils.compileFilter(filter, isPostgreSql, fields))
        .collect(Collectors.joining(" AND "));
  }

  /** Converts value to SQL expression. */
  static String compileValue(Object value, boolean isPostgreSql) {
    return compileValue(value, /*arrayStart=*/ '[', /*arrayEnd=*/ ']', isPostgreSql);
  }

  /** Converts value to SQL expression customizing array start/end values. */
  static String compileValue(Object value, char arrayStart, char arrayEnd, boolean isPostgreSql) {
    if (value == null) {
      return null;
    }
    if (value instanceof String) {
      return "'" + escape((String) value) + "'";
    }
    if (value instanceof Date || value instanceof LocalDate) {
      return "DATE '" + value + "'";
    }
    if (isPostgreSql && value instanceof Timestamp) {
      return "'" + value + "-0'";
    } else if (isPostgreSql && value instanceof Instant) {
      return "'" + value + "'";
    } else if (value instanceof Timestamp || value instanceof Instant) {
      return value.toString().endsWith("Z")
          ? "TIMESTAMP '" + value + "'"
          : "TIMESTAMP '" + value + "Z'";
    }
    if (value instanceof byte[] || value instanceof Byte[]) {
      return isPostgreSql
          ? "'" + escape(new String((byte[]) value, StandardCharsets.UTF_8)) + "'"
          : "b'" + escape(new String((byte[]) value, StandardCharsets.UTF_8)) + "'";
    }
    if (value instanceof BigDecimal) {
      return "NUMERIC '" + value + "'";
    }
    if (isPostgreSql && value instanceof Double) {
      return "'" + value + "'";
    } else if (value instanceof Double) {
      return "CAST(\"" + value + "\" AS FLOAT64)";
    }
    if (value instanceof Object[]) {
      return Arrays.stream((Object[]) value)
          .map(v -> SparkFilterUtils.compileValue(v, isPostgreSql))
          .collect(
              Collectors.joining(
                  ", ", Character.toString(arrayStart), Character.toString(arrayEnd)));
    }
    return value.toString();
  }

  static String escape(String value) {
    return value.replace("'", "\\'");
  }

  static String quote(String value, boolean isPostgreSql, Map<String, StructField> fields) {
    return quote(value, isPostgreSql, fields, /* isNullCheck= */ false);
  }

  static String quote(
      String value, boolean isPostgreSql, Map<String, StructField> fields, boolean isNullCheck) {
    if (!isPostgreSql && isJson(fields, value) && !isNullCheck) {
      return "TO_JSON_STRING(`" + value + "`)";
    }
    if (isPostgreSql && isJsonb(fields, value)) {
      return "CAST(\"" + value + "\" AS VARCHAR)";
    }
    if (isPostgreSql) {
      return "\"" + value + "\"";
    }
    return "`" + value + "`";
  }

  static boolean isJson(Map<String, StructField> fields, String fieldName) {
    return isJson(fields, fieldName, "json");
  }

  static boolean isJsonb(Map<String, StructField> fields, String fieldName) {
    return isJson(fields, fieldName, "jsonb");
  }

  static boolean isJsonb(StructField field) {
    return isJson(field, "jsonb");
  }

  static boolean isJson(
      Map<String, StructField> fields, String fieldName, String fieldLikeMetadataType) {
    if (fields.containsKey(fieldName)) {
      return isJson(fields.get(fieldName), fieldLikeMetadataType);
    }
    return false;
  }

  static boolean isJson(StructField field, String fieldLikeMetadataType) {
    return field.dataType() == DataTypes.StringType
        && field.metadata() != null
        && field.metadata().contains(SpannerUtils.COLUMN_TYPE)
        && fieldLikeMetadataType.equals(field.metadata().getString(SpannerUtils.COLUMN_TYPE));
  }
}
