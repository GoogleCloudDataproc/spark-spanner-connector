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
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/*
 * Copied from BigQuery v2 Spark Connector.
 */
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
    return Optional.ofNullable(fields.get(fieldName))
        .filter(
            field ->
                ((field.dataType() instanceof StructType)
                    || (field.dataType() instanceof ArrayType)))
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

  public static String getCompiledFilter(boolean pushAllFilters, Filter... pushedFilters) {
    String compiledPushedFilter =
        compileFilters(handledFilters(pushAllFilters, ImmutableList.copyOf(pushedFilters)));
    return Stream.of(
            compiledPushedFilter.length() == 0
                ? Optional.empty()
                : Optional.of(compiledPushedFilter))
        .filter(Optional::isPresent)
        .map(filter -> "(" + filter.get() + ")")
        .collect(Collectors.joining(" AND "));
  }

  // Mostly copied from JDBCRDD.scala
  public static String compileFilter(Filter filter) {
    if (filter instanceof EqualTo) {
      EqualTo equalTo = (EqualTo) filter;
      return format("%s = %s", quote(equalTo.attribute()), compileValue(equalTo.value()));
    }
    if (filter instanceof EqualNullSafe) {
      EqualNullSafe equalNullSafe = (EqualNullSafe) filter;
      String left = quote(equalNullSafe.attribute());
      String right = compileValue(equalNullSafe.value());
      return format(
          "%1$s IS NULL AND %2$s IS NULL OR %1$s IS NOT NULL AND %2$s IS NOT NULL AND %1$s = %2$s",
          left, right);
    }
    if (filter instanceof GreaterThan) {
      GreaterThan greaterThan = (GreaterThan) filter;
      return format("%s > %s", quote(greaterThan.attribute()), compileValue(greaterThan.value()));
    }
    if (filter instanceof GreaterThanOrEqual) {
      GreaterThanOrEqual greaterThanOrEqual = (GreaterThanOrEqual) filter;
      return format(
          "%s >= %s",
          quote(greaterThanOrEqual.attribute()), compileValue(greaterThanOrEqual.value()));
    }
    if (filter instanceof LessThan) {
      LessThan lessThan = (LessThan) filter;
      return format("%s < %s", quote(lessThan.attribute()), compileValue(lessThan.value()));
    }
    if (filter instanceof LessThanOrEqual) {
      LessThanOrEqual lessThanOrEqual = (LessThanOrEqual) filter;
      return format(
          "%s <= %s", quote(lessThanOrEqual.attribute()), compileValue(lessThanOrEqual.value()));
    }
    if (filter instanceof In) {
      In in = (In) filter;
      return format(
          "%s IN %s",
          quote(in.attribute()), compileValue(in.values(), /*arrayStart=*/ '(', /*arrayEnd=*/ ')'));
    }
    if (filter instanceof IsNull) {
      IsNull isNull = (IsNull) filter;
      return format("%s IS NULL", quote(isNull.attribute()));
    }
    if (filter instanceof IsNotNull) {
      IsNotNull isNotNull = (IsNotNull) filter;
      return format("%s IS NOT NULL", quote(isNotNull.attribute()));
    }
    if (filter instanceof And) {
      And and = (And) filter;
      return format("((%s) AND (%s))", compileFilter(and.left()), compileFilter(and.right()));
    }
    if (filter instanceof Or) {
      Or or = (Or) filter;
      return format("((%s) OR (%s))", compileFilter(or.left()), compileFilter(or.right()));
    }
    if (filter instanceof Not) {
      Not not = (Not) filter;
      return format("(NOT (%s))", compileFilter(not.child()));
    }
    if (filter instanceof StringStartsWith) {
      StringStartsWith stringStartsWith = (StringStartsWith) filter;
      return format(
          "%s LIKE '%s%%'", quote(stringStartsWith.attribute()), escape(stringStartsWith.value()));
    }
    if (filter instanceof StringEndsWith) {
      StringEndsWith stringEndsWith = (StringEndsWith) filter;
      return format(
          "%s LIKE '%%%s'", quote(stringEndsWith.attribute()), escape(stringEndsWith.value()));
    }
    if (filter instanceof StringContains) {
      StringContains stringContains = (StringContains) filter;
      return format(
          "%s LIKE '%%%s%%'", quote(stringContains.attribute()), escape(stringContains.value()));
    }

    throw new IllegalArgumentException(format("Invalid filter: %s", filter));
  }

  public static String compileFilters(Iterable<Filter> filters) {
    return StreamSupport.stream(filters.spliterator(), false)
        .map(SparkFilterUtils::compileFilter)
        .sorted()
        .collect(Collectors.joining(" AND "));
  }

  /** Converts value to SQL expression. */
  static String compileValue(Object value) {
    return compileValue(value, /*arrayStart=*/ '[', /*arrayEnd=*/ ']');
  }

  /** Converts value to SQL expression customizing array start/end values. */
  static String compileValue(Object value, char arrayStart, char arrayEnd) {
    if (value == null) {
      return null;
    }
    if (value instanceof String) {
      return "'" + escape((String) value) + "'";
    }
    if (value instanceof Date) {
      return "DATE '" + value + "'";
    }
    if (value instanceof Timestamp) {
      return "TIMESTAMP '" + value + "'";
    }
    if (value instanceof Object[]) {
      return Arrays.stream((Object[]) value)
          .map(SparkFilterUtils::compileValue)
          .collect(
              Collectors.joining(
                  ", ", Character.toString(arrayStart), Character.toString(arrayEnd)));
    }
    return value.toString();
  }

  static String escape(String value) {
    return value.replace("'", "\\'");
  }

  static String quote(String value) {
    return "`" + value + "`";
  }
}