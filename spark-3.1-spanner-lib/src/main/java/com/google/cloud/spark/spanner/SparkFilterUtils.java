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

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spark.spanner.binding.ParameterRef;
import com.google.cloud.spark.spanner.binding.ParameterRegistry;
import com.google.cloud.spark.spanner.planning.expression.LiteralExpr;
import com.google.cloud.spark.spanner.rendering.RenderResult;
import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.Not;
import org.apache.spark.sql.sources.Or;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
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

  public static RenderResult getCompiledFilter(
      boolean pushAllFilters,
      Optional<String> configFilter,
      boolean isPostgreSql,
      Map<String, StructField> fields,
      Filter... pushedFilters) {
    return getCompiledFilter(
        pushAllFilters, configFilter, isPostgreSql, fields, null, pushedFilters);
  }

  public static RenderResult getCompiledFilter(
      boolean pushAllFilters,
      Optional<String> configFilter,
      boolean isPostgreSql,
      Map<String, StructField> fields,
      @Nullable String tableQualifier,
      Filter... pushedFilters) {
    return getCompiledFilter(
        pushAllFilters,
        configFilter,
        isPostgreSql,
        fields,
        tableQualifier,
        ParameterRegistry.create(isPostgreSql ? Dialect.POSTGRESQL : Dialect.GOOGLE_STANDARD_SQL),
        pushedFilters);
  }

  public static RenderResult getCompiledFilter(
      boolean pushAllFilters,
      Optional<String> configFilter,
      boolean isPostgreSql,
      Map<String, StructField> fields,
      @Nullable String tableQualifier,
      ParameterRegistry parameterRegistry,
      Filter... pushedFilters) {
    RenderResult compiledPushedFilter =
        compileFilters(
            handledFilters(pushAllFilters, ImmutableList.copyOf(pushedFilters)),
            isPostgreSql,
            fields,
            tableQualifier,
            parameterRegistry);
    String sql =
        Stream.of(
                configFilter,
                compiledPushedFilter.getSql().isEmpty()
                    ? Optional.<String>empty()
                    : Optional.of(compiledPushedFilter.getSql()))
            .filter(Optional::isPresent)
            .map(filter -> "(" + filter.get() + ")")
            .collect(Collectors.joining(" AND "));
    return new RenderResult(sql, compiledPushedFilter.getBindings());
  }

  // Mostly copied from JDBCRDD.scala
  public static RenderResult compileFilter(
      Filter filter,
      boolean isPostgreSql,
      Map<String, StructField> fields,
      @Nullable String tableQualifier) {
    return compileFilter(
        filter,
        isPostgreSql,
        fields,
        tableQualifier,
        ParameterRegistry.create(isPostgreSql ? Dialect.POSTGRESQL : Dialect.GOOGLE_STANDARD_SQL));
  }

  public static RenderResult compileFilter(
      Filter filter,
      boolean isPostgreSql,
      Map<String, StructField> fields,
      @Nullable String tableQualifier,
      ParameterRegistry parameterRegistry) {
    if (filter instanceof EqualTo) {
      EqualTo equalTo = (EqualTo) filter;
      ParameterRef ref = parameterRegistry.nextParameter();
      return new RenderResult(
          format(
              "%s = %s",
              quote(equalTo.attribute(), isPostgreSql, fields, tableQualifier),
              formatParam(ref, isPostgreSql)),
          Collections.singletonMap(
              ref.getBindName(), toLiteralExpr(equalTo.attribute(), equalTo.value(), fields)));
    }
    if (filter instanceof EqualNullSafe) {
      EqualNullSafe equalNullSafe = (EqualNullSafe) filter;
      String leftNullCheck =
          quote(
              equalNullSafe.attribute(),
              isPostgreSql,
              fields,
              /* isNullCheck= */ true,
              tableQualifier);
      String leftValueCheck =
          quote(equalNullSafe.attribute(), isPostgreSql, fields, tableQualifier);
      ParameterRef ref = parameterRegistry.nextParameter();
      String right = formatParam(ref, isPostgreSql);
      return new RenderResult(
          format(
              "%1$s IS NULL AND %2$s IS NULL OR %1$s IS NOT NULL AND %2$s IS NOT NULL AND %3$s = %2$s",
              leftNullCheck, right, leftValueCheck),
          Collections.singletonMap(
              ref.getBindName(),
              toLiteralExpr(equalNullSafe.attribute(), equalNullSafe.value(), fields)));
    }
    if (filter instanceof GreaterThan) {
      GreaterThan greaterThan = (GreaterThan) filter;
      ParameterRef ref = parameterRegistry.nextParameter();
      return new RenderResult(
          format(
              "%s > %s",
              quote(greaterThan.attribute(), isPostgreSql, fields, tableQualifier),
              formatParam(ref, isPostgreSql)),
          Collections.singletonMap(
              ref.getBindName(),
              toLiteralExpr(greaterThan.attribute(), greaterThan.value(), fields)));
    }
    if (filter instanceof GreaterThanOrEqual) {
      GreaterThanOrEqual greaterThanOrEqual = (GreaterThanOrEqual) filter;
      ParameterRef ref = parameterRegistry.nextParameter();
      return new RenderResult(
          format(
              "%s >= %s",
              quote(greaterThanOrEqual.attribute(), isPostgreSql, fields, tableQualifier),
              formatParam(ref, isPostgreSql)),
          Collections.singletonMap(
              ref.getBindName(),
              toLiteralExpr(greaterThanOrEqual.attribute(), greaterThanOrEqual.value(), fields)));
    }
    if (filter instanceof LessThan) {
      LessThan lessThan = (LessThan) filter;
      ParameterRef ref = parameterRegistry.nextParameter();
      return new RenderResult(
          format(
              "%s < %s",
              quote(lessThan.attribute(), isPostgreSql, fields, tableQualifier),
              formatParam(ref, isPostgreSql)),
          Collections.singletonMap(
              ref.getBindName(), toLiteralExpr(lessThan.attribute(), lessThan.value(), fields)));
    }
    if (filter instanceof LessThanOrEqual) {
      LessThanOrEqual lessThanOrEqual = (LessThanOrEqual) filter;
      ParameterRef ref = parameterRegistry.nextParameter();
      return new RenderResult(
          format(
              "%s <= %s",
              quote(lessThanOrEqual.attribute(), isPostgreSql, fields, tableQualifier),
              formatParam(ref, isPostgreSql)),
          Collections.singletonMap(
              ref.getBindName(),
              toLiteralExpr(lessThanOrEqual.attribute(), lessThanOrEqual.value(), fields)));
    }
    if (filter instanceof In) {
      In in = (In) filter;
      Map<String, LiteralExpr> bindings = new LinkedHashMap<>();
      List<String> placeholders = new ArrayList<>(in.values().length);
      for (Object val : in.values()) {
        ParameterRef ref = parameterRegistry.nextParameter();
        placeholders.add(formatParam(ref, isPostgreSql));
        bindings.put(ref.getBindName(), toLiteralExpr(in.attribute(), val, fields));
      }
      return new RenderResult(
          format(
              "%s IN (%s)",
              quote(in.attribute(), isPostgreSql, fields, tableQualifier),
              String.join(", ", placeholders)),
          bindings);
    }
    if (filter instanceof IsNull) {
      IsNull isNull = (IsNull) filter;
      return new RenderResult(
          format(
              "%s IS NULL",
              quote(
                  isNull.attribute(),
                  isPostgreSql,
                  fields,
                  /* isNullCheck= */ true,
                  tableQualifier)),
          Collections.emptyMap());
    }
    if (filter instanceof IsNotNull) {
      IsNotNull isNotNull = (IsNotNull) filter;
      return new RenderResult(
          format(
              "%s IS NOT NULL",
              quote(
                  isNotNull.attribute(),
                  isPostgreSql,
                  fields,
                  /* isNullCheck= */ true,
                  tableQualifier)),
          Collections.emptyMap());
    }
    if (filter instanceof And) {
      And and = (And) filter;
      RenderResult left =
          compileFilter(and.left(), isPostgreSql, fields, tableQualifier, parameterRegistry);
      RenderResult right =
          compileFilter(and.right(), isPostgreSql, fields, tableQualifier, parameterRegistry);
      Map<String, LiteralExpr> bindings = new LinkedHashMap<>(left.getBindings());
      bindings.putAll(right.getBindings());
      return new RenderResult(format("((%s) AND (%s))", left.getSql(), right.getSql()), bindings);
    }
    if (filter instanceof Or) {
      Or or = (Or) filter;
      RenderResult left =
          compileFilter(or.left(), isPostgreSql, fields, tableQualifier, parameterRegistry);
      RenderResult right =
          compileFilter(or.right(), isPostgreSql, fields, tableQualifier, parameterRegistry);
      Map<String, LiteralExpr> bindings = new LinkedHashMap<>(left.getBindings());
      bindings.putAll(right.getBindings());
      return new RenderResult(format("((%s) OR (%s))", left.getSql(), right.getSql()), bindings);
    }
    if (filter instanceof Not) {
      Not not = (Not) filter;
      RenderResult child =
          compileFilter(not.child(), isPostgreSql, fields, tableQualifier, parameterRegistry);
      return new RenderResult(format("(NOT (%s))", child.getSql()), child.getBindings());
    }
    if (filter instanceof StringStartsWith) {
      StringStartsWith stringStartsWith = (StringStartsWith) filter;
      ParameterRef ref = parameterRegistry.nextParameter();
      String pattern = escapeLikePattern(stringStartsWith.value()) + "%";
      return new RenderResult(
          format(
              "%s LIKE %s",
              quote(stringStartsWith.attribute(), isPostgreSql, fields, tableQualifier),
              formatParam(ref, isPostgreSql)),
          Collections.singletonMap(
              ref.getBindName(), new LiteralExpr(pattern, DataTypes.StringType, "STRING")));
    }
    if (filter instanceof StringEndsWith) {
      StringEndsWith stringEndsWith = (StringEndsWith) filter;
      ParameterRef ref = parameterRegistry.nextParameter();
      String pattern = "%" + escapeLikePattern(stringEndsWith.value());
      return new RenderResult(
          format(
              "%s LIKE %s",
              quote(stringEndsWith.attribute(), isPostgreSql, fields, tableQualifier),
              formatParam(ref, isPostgreSql)),
          Collections.singletonMap(
              ref.getBindName(), new LiteralExpr(pattern, DataTypes.StringType, "STRING")));
    }
    if (filter instanceof StringContains) {
      StringContains stringContains = (StringContains) filter;
      ParameterRef ref = parameterRegistry.nextParameter();
      String pattern = "%" + escapeLikePattern(stringContains.value()) + "%";
      return new RenderResult(
          format(
              "%s LIKE %s",
              quote(stringContains.attribute(), isPostgreSql, fields, tableQualifier),
              formatParam(ref, isPostgreSql)),
          Collections.singletonMap(
              ref.getBindName(), new LiteralExpr(pattern, DataTypes.StringType, "STRING")));
    }

    throw new IllegalArgumentException(format("Invalid filter: %s", filter));
  }

  public static RenderResult compileFilters(
      Iterable<Filter> filters,
      boolean isPostgreSql,
      Map<String, StructField> fields,
      @Nullable String tableQualifier) {
    return compileFilters(
        filters,
        isPostgreSql,
        fields,
        tableQualifier,
        ParameterRegistry.create(isPostgreSql ? Dialect.POSTGRESQL : Dialect.GOOGLE_STANDARD_SQL));
  }

  public static RenderResult compileFilters(
      Iterable<Filter> filters,
      boolean isPostgreSql,
      Map<String, StructField> fields,
      @Nullable String tableQualifier,
      ParameterRegistry parameterRegistry) {
    List<String> sqlParts = new ArrayList<>();
    Map<String, LiteralExpr> bindings = new LinkedHashMap<>();
    for (Filter filter : filters) {
      RenderResult result =
          compileFilter(filter, isPostgreSql, fields, tableQualifier, parameterRegistry);
      sqlParts.add(result.getSql());
      bindings.putAll(result.getBindings());
    }
    return new RenderResult(String.join(" AND ", sqlParts), bindings);
  }

  private static String formatParam(ParameterRef ref, boolean isPostgreSql) {
    return isPostgreSql ? "$" + ref.getSqlName() : "@" + ref.getSqlName();
  }

  static LiteralExpr toLiteralExpr(
      String attribute, @Nullable Object value, Map<String, StructField> fields) {
    String spannerType = isUuid(fields, attribute) ? "UUID" : null;
    if (value == null) {
      DataType sparkType = DataTypes.StringType;
      if (fields != null && fields.containsKey(attribute)) {
        StructField field = fields.get(attribute);
        if (field != null && field.dataType() != null) {
          sparkType = field.dataType();
        }
      }
      return new LiteralExpr(null, sparkType, spannerType);
    }
    if (value instanceof String) {
      return new LiteralExpr(
          value, DataTypes.StringType, spannerType != null ? spannerType : "STRING");
    }
    if (value instanceof Long) {
      return new LiteralExpr(value, DataTypes.LongType, "INT64");
    }
    if (value instanceof Integer) {
      return new LiteralExpr(value, DataTypes.IntegerType, "INT64");
    }
    if (value instanceof Short) {
      return new LiteralExpr(value, DataTypes.ShortType, "INT64");
    }
    if (value instanceof Byte) {
      return new LiteralExpr(value, DataTypes.ByteType, "INT64");
    }
    if (value instanceof Boolean) {
      return new LiteralExpr(value, DataTypes.BooleanType, "BOOL");
    }
    if (value instanceof Double) {
      return new LiteralExpr(value, DataTypes.DoubleType, "FLOAT64");
    }
    if (value instanceof Float) {
      return new LiteralExpr(value, DataTypes.FloatType, "FLOAT64");
    }
    if (value instanceof BigDecimal) {
      return new LiteralExpr(value, DataTypes.createDecimalType(38, 9), "NUMERIC");
    }
    if (value instanceof Decimal) {
      return new LiteralExpr(
          ((Decimal) value).toJavaBigDecimal(), DataTypes.createDecimalType(38, 9), "NUMERIC");
    }
    if (value instanceof Date || value instanceof LocalDate) {
      return new LiteralExpr(value, DataTypes.DateType, "DATE");
    }
    if (value instanceof Timestamp || value instanceof Instant) {
      return new LiteralExpr(value, DataTypes.TimestampType, "TIMESTAMP");
    }
    if (value instanceof byte[] || value instanceof Byte[]) {
      return new LiteralExpr(value, DataTypes.BinaryType, "BYTES");
    }
    throw new IllegalArgumentException(
        format("Unsupported filter value type: %s", value.getClass().getName()));
  }

  static String escapeLikePattern(String value) {
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_");
  }

  public static String quoteIdentifier(String identifier, boolean isPostgreSql) {
    if (isPostgreSql) {
      return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }
    return "`" + identifier.replace("`", "``") + "`";
  }

  static String quote(
      String value,
      boolean isPostgreSql,
      Map<String, StructField> fields,
      @Nullable String tableQualifier) {
    return quote(value, isPostgreSql, fields, /* isNullCheck= */ false, tableQualifier);
  }

  static String quote(
      String value,
      boolean isPostgreSql,
      Map<String, StructField> fields,
      boolean isNullCheck,
      @Nullable String tableQualifier) {
    final String quotedIdentifier =
        tableQualifier == null
            ? quoteIdentifier(value, isPostgreSql)
            : quoteIdentifier(tableQualifier, isPostgreSql)
                + "."
                + quoteIdentifier(value, isPostgreSql);
    if (!isPostgreSql && isJson(fields, value) && !isNullCheck) {
      return "TO_JSON_STRING(" + quotedIdentifier + ")";
    }
    if (isPostgreSql && isJsonb(fields, value)) {
      return "CAST(" + quotedIdentifier + " AS VARCHAR)";
    }
    return quotedIdentifier;
  }

  static boolean isJson(Map<String, StructField> fields, String fieldName) {
    return isJson(fields, fieldName, "json");
  }

  static boolean isJsonb(Map<String, StructField> fields, String fieldName) {
    return isJson(fields, fieldName, "jsonb");
  }

  static boolean isUuid(Map<String, StructField> fields, String fieldName) {
    return isJson(fields, fieldName, "uuid");
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

  public static String filtersToString(Filter[] filters) {
    if (filters == null) {
      return "null";
    }

    if (filters.length == 0) {
      return "[]";
    }

    StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < filters.length; i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(filterToString(filters[i]));
    }
    sb.append("]");
    return sb.toString();
  }

  private static String filterToString(Filter filter) {
    if (filter == null) {
      return "null";
    }

    if (filter instanceof And) {
      And and = (And) filter;
      return "(" + filterToString(and.left()) + " AND " + filterToString(and.right()) + ")";
    }

    if (filter instanceof Or) {
      Or or = (Or) filter;
      return "(" + filterToString(or.left()) + " OR " + filterToString(or.right()) + ")";
    }

    if (filter instanceof Not) {
      Not not = (Not) filter;
      return "(NOT " + filterToString(not.child()) + ")";
    }

    // EqualTo, GreaterThan, In, IsNull, etc.
    return filter.toString();
  }
}
