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

// Copied from spark-bigquery

package com.google.cloud.spark.spanner;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Value;
import com.google.cloud.spark.spanner.binding.SpannerTypeBinder;
import com.google.cloud.spark.spanner.planning.expression.LiteralExpr;
import com.google.cloud.spark.spanner.rendering.RenderResult;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.text.ParseException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import scala.collection.mutable.WrappedArray;

@RunWith(Parameterized.class)
public class SparkFilterUtilsTest {
  private boolean pushAllFilters = true;
  private boolean isPostgreSql;
  private static final Map<String, StructField> EMPTY_FIELDS = ImmutableMap.of();

  private String getQuotedSplitter() {
    return isPostgreSql ? "\"" : "`";
  }

  private String param(int index) {
    return isPostgreSql ? "$" + index : "@p" + index;
  }

  // The function parse the {0} into the quoted splitter and translate the '' into single quote '.
  private String parseQuotedSplitter(String s) {
    return MessageFormat.format(s, getQuotedSplitter());
  }

  private Statement toStatement(RenderResult result) {
    Dialect dialect = isPostgreSql ? Dialect.POSTGRESQL : Dialect.GOOGLE_STANDARD_SQL;
    Statement.Builder builder = Statement.newBuilder(result.getSql());
    for (Map.Entry<String, LiteralExpr> entry : result.getBindings().entrySet()) {
      SpannerTypeBinder.bind(builder, entry.getKey(), entry.getValue(), dialect);
    }
    return builder.build();
  }

  @Parameterized.Parameters(name = "{index}: isPostgreSql={0}")
  public static Iterable<Object[]> data() {
    return ImmutableList.of(new Object[] {false}, new Object[] {true});
  }

  public SparkFilterUtilsTest(boolean isPostgreSql) {
    this.isPostgreSql = isPostgreSql;
  }

  @Test
  public void testValidFilters() {
    ArrayList<Filter> validFilters =
        new ArrayList(
            Arrays.asList(
                EqualTo.apply("foo", "manatee"),
                EqualNullSafe.apply("foo", "manatee"),
                GreaterThan.apply("foo", "aardvark"),
                GreaterThanOrEqual.apply("bar", 2),
                LessThan.apply("foo", "zebra"),
                LessThanOrEqual.apply("bar", 1),
                In.apply("foo", new Object[] {1, 2, 3}),
                IsNull.apply("foo"),
                IsNotNull.apply("foo"),
                And.apply(IsNull.apply("foo"), IsNotNull.apply("bar")),
                Not.apply(IsNull.apply("foo")),
                StringStartsWith.apply("foo", "abc"),
                StringEndsWith.apply("foo", "def"),
                StringContains.apply("foo", "abcdef")));
    validFilters.add(Or.apply(IsNull.apply("foo"), IsNotNull.apply("foo")));
    validFilters.forEach(
        f -> assertThat(SparkFilterUtils.unhandledFilters(pushAllFilters, f)).isEmpty());
  }

  @Test
  public void testMultipleValidFiltersAreHandled() {
    Filter valid1 = EqualTo.apply("foo", "bar");
    Filter valid2 = EqualTo.apply("bar", 1);
    assertThat(SparkFilterUtils.unhandledFilters(pushAllFilters, valid1, valid2)).isEmpty();
  }

  @Test
  public void testInvalidFilters() {
    Filter valid1 = EqualTo.apply("foo", "bar");
    Filter valid2 = EqualTo.apply("bar", 1);
    Filter valid3 = EqualNullSafe.apply("foo", "bar");
    Filter valid4 = Or.apply(IsNull.apply("foo"), IsNotNull.apply("foo"));
    Iterable<Filter> unhandled =
        SparkFilterUtils.unhandledFilters(pushAllFilters, valid1, valid2, valid3, valid4);
    assertThat(unhandled).isEmpty();
  }

  @Test
  public void testNewFilterBehaviourWithFilterOption() {
    checkFilters(
        pushAllFilters,
        "(f>1)",
        parseQuotedSplitter("(f>1) AND ({0}a{0} > " + param(1) + ")"),
        Optional.of("f>1"),
        GreaterThan.apply("a", 2));
  }

  @Test
  public void testNewFilterBehaviourNoFilterOption() {
    checkFilters(
        pushAllFilters,
        "",
        parseQuotedSplitter("({0}a{0} > " + param(1) + ")"),
        Optional.empty(),
        GreaterThan.apply("a", 2));
  }

  private void checkFilters(
      boolean pushAllFilters,
      String resultWithoutFilters,
      String resultWithFilters,
      Optional<String> configFilter,
      Filter... filters) {
    RenderResult result1 =
        SparkFilterUtils.getCompiledFilter(
            pushAllFilters, configFilter, isPostgreSql, EMPTY_FIELDS);
    assertThat(result1.getSql()).isEqualTo(resultWithoutFilters);
    assertThat(result1.getBindings()).isEmpty();
    RenderResult result2 =
        SparkFilterUtils.getCompiledFilter(
            pushAllFilters, configFilter, isPostgreSql, EMPTY_FIELDS, filters);
    assertThat(result2.getSql()).isEqualTo(resultWithFilters);
  }

  @Test
  public void testStringFilters() {
    RenderResult startsWith =
        SparkFilterUtils.compileFilter(
            StringStartsWith.apply("foo", "bar"), isPostgreSql, EMPTY_FIELDS, null);
    assertThat(startsWith.getSql()).isEqualTo(parseQuotedSplitter("{0}foo{0} LIKE " + param(1)));
    assertThat(toStatement(startsWith).getParameters().get("p1")).isEqualTo(Value.string("bar%"));

    RenderResult endsWith =
        SparkFilterUtils.compileFilter(
            StringEndsWith.apply("foo", "bar"), isPostgreSql, EMPTY_FIELDS, null);
    assertThat(endsWith.getSql()).isEqualTo(parseQuotedSplitter("{0}foo{0} LIKE " + param(1)));
    assertThat(toStatement(endsWith).getParameters().get("p1")).isEqualTo(Value.string("%bar"));

    RenderResult contains =
        SparkFilterUtils.compileFilter(
            StringContains.apply("foo", "bar"), isPostgreSql, EMPTY_FIELDS, null);
    assertThat(contains.getSql()).isEqualTo(parseQuotedSplitter("{0}foo{0} LIKE " + param(1)));
    assertThat(toStatement(contains).getParameters().get("p1")).isEqualTo(Value.string("%bar%"));

    RenderResult startsWithQuote =
        SparkFilterUtils.compileFilter(
            StringStartsWith.apply("foo", "b'ar"), isPostgreSql, EMPTY_FIELDS, null);
    assertThat(startsWithQuote.getSql())
        .isEqualTo(parseQuotedSplitter("{0}foo{0} LIKE " + param(1)));
    assertThat(toStatement(startsWithQuote).getParameters().get("p1"))
        .isEqualTo(Value.string("b'ar%"));

    RenderResult endsWithQuote =
        SparkFilterUtils.compileFilter(
            StringEndsWith.apply("foo", "b'ar"), isPostgreSql, EMPTY_FIELDS, null);
    assertThat(endsWithQuote.getSql()).isEqualTo(parseQuotedSplitter("{0}foo{0} LIKE " + param(1)));
    assertThat(toStatement(endsWithQuote).getParameters().get("p1"))
        .isEqualTo(Value.string("%b'ar"));

    RenderResult containsQuote =
        SparkFilterUtils.compileFilter(
            StringContains.apply("foo", "b'ar"), isPostgreSql, EMPTY_FIELDS, null);
    assertThat(containsQuote.getSql()).isEqualTo(parseQuotedSplitter("{0}foo{0} LIKE " + param(1)));
    assertThat(toStatement(containsQuote).getParameters().get("p1"))
        .isEqualTo(Value.string("%b'ar%"));
  }

  @Test
  public void testStringFiltersEscapeLikeWildcards() {
    RenderResult startsWith =
        SparkFilterUtils.compileFilter(
            StringStartsWith.apply("foo", "100%_\\test"), isPostgreSql, EMPTY_FIELDS, null);
    assertThat(startsWith.getSql()).isEqualTo(parseQuotedSplitter("{0}foo{0} LIKE " + param(1)));
    assertThat(toStatement(startsWith).getParameters().get("p1"))
        .isEqualTo(Value.string("100\\%\\_\\\\test%"));

    RenderResult endsWith =
        SparkFilterUtils.compileFilter(
            StringEndsWith.apply("foo", "100%_\\test"), isPostgreSql, EMPTY_FIELDS, null);
    assertThat(endsWith.getSql()).isEqualTo(parseQuotedSplitter("{0}foo{0} LIKE " + param(1)));
    assertThat(toStatement(endsWith).getParameters().get("p1"))
        .isEqualTo(Value.string("%100\\%\\_\\\\test"));

    RenderResult contains =
        SparkFilterUtils.compileFilter(
            StringContains.apply("foo", "100%_\\test"), isPostgreSql, EMPTY_FIELDS, null);
    assertThat(contains.getSql()).isEqualTo(parseQuotedSplitter("{0}foo{0} LIKE " + param(1)));
    assertThat(toStatement(contains).getParameters().get("p1"))
        .isEqualTo(Value.string("%100\\%\\_\\\\test%"));
  }

  @Test
  public void testSqlInjectionPayloadsAreBoundAsParameters() {
    String gsqlPayload = "\\' OR TRUE --";
    RenderResult eqResult =
        SparkFilterUtils.compileFilter(
            EqualTo.apply("foo", gsqlPayload), isPostgreSql, EMPTY_FIELDS, null);
    assertThat(eqResult.getSql()).isEqualTo(parseQuotedSplitter("{0}foo{0} = " + param(1)));
    assertThat(eqResult.getSql()).doesNotContain("OR TRUE");
    assertThat(toStatement(eqResult).getParameters().get("p1"))
        .isEqualTo(Value.string(gsqlPayload));

    String pgPayload = "' OR '1'='1";
    RenderResult inResult =
        SparkFilterUtils.compileFilter(
            In.apply("foo", new Object[] {gsqlPayload, pgPayload}),
            isPostgreSql,
            EMPTY_FIELDS,
            null);
    assertThat(inResult.getSql())
        .isEqualTo(parseQuotedSplitter("{0}foo{0} IN (" + param(1) + ", " + param(2) + ")"));
    Statement stmt = toStatement(inResult);
    assertThat(stmt.getParameters().get("p1")).isEqualTo(Value.string(gsqlPayload));
    assertThat(stmt.getParameters().get("p2")).isEqualTo(Value.string(pgPayload));
  }

  @Test
  public void testCompileFilterWithNullFields() {
    RenderResult eqString =
        SparkFilterUtils.compileFilter(EqualTo.apply("foo", "bar"), isPostgreSql, null, null);
    assertThat(eqString.getSql()).isEqualTo(parseQuotedSplitter("{0}foo{0} = " + param(1)));
    assertThat(toStatement(eqString).getParameters().get("p1")).isEqualTo(Value.string("bar"));

    RenderResult eqNull =
        SparkFilterUtils.compileFilter(EqualNullSafe.apply("foo", null), isPostgreSql, null, null);
    assertThat(eqNull.getSql()).isEqualTo(parseQuotedSplitter("({0}foo{0} IS NULL)"));
    assertThat(eqNull.getBindings()).isEmpty();

    assertThat(SparkFilterUtils.isUuid(null, "foo")).isFalse();
    assertThat(SparkFilterUtils.isJson((Map<String, StructField>) null, "foo")).isFalse();
    assertThat(SparkFilterUtils.isJson((StructField) null, "json")).isFalse();
    assertThat(SparkFilterUtils.isJsonb((Map<String, StructField>) null, "foo")).isFalse();
    assertThat(SparkFilterUtils.isTopLevelFieldHandled(false, EqualTo.apply("foo", "bar"), null))
        .isTrue();
  }

  @Test
  public void testQuoteEscapesIdentifierDelimiters() {
    if (isPostgreSql) {
      assertThat(SparkFilterUtils.quote("a\"b", true, EMPTY_FIELDS, null)).isEqualTo("\"a\"\"b\"");
      assertThat(SparkFilterUtils.quote("a\"b", true, EMPTY_FIELDS, "t\"1"))
          .isEqualTo("\"t\"\"1\".\"a\"\"b\"");
      assertThat(
              SparkFilterUtils.quote(
                  "field\"1", true, ImmutableMap.of("field\"1", structFieldWithJson()), null))
          .isEqualTo("CAST(\"field\"\"1\" AS VARCHAR)");
    } else {
      assertThat(SparkFilterUtils.quote("a`b", false, EMPTY_FIELDS, null)).isEqualTo("`a``b`");
      assertThat(SparkFilterUtils.quote("a`b", false, EMPTY_FIELDS, "t`1"))
          .isEqualTo("`t``1`.`a``b`");
      assertThat(
              SparkFilterUtils.quote(
                  "field`1", false, ImmutableMap.of("field`1", structFieldWithJson()), null))
          .isEqualTo("TO_JSON_STRING(`field``1`)");
    }
  }

  @Test
  public void testArrayFilters() {
    List<String> testString = ImmutableList.of("test");

    assertThat(
            SparkFilterUtils.isTopLevelFieldHandled(
                false,
                EqualTo.apply("array", WrappedArray.make(testString.toArray(new String[0]))),
                ImmutableMap.of(
                    "array",
                    new StructField(
                        "array", DataTypes.createArrayType(DataTypes.StringType), true, null))))
        .isEqualTo(false);
  }

  @Test
  public void postgresqlShouldSkipJsonbForInFilter() {
    assertThat(
            SparkFilterUtils.isTopLevelFieldHandled(
                false,
                In.apply("field", new Object[] {"Json", "test"}),
                ImmutableMap.of("field", structFieldWithJson())))
        .isEqualTo(isPostgreSql ? false : true);
  }

  @Test
  public void postgresqlShouldNotSkipJsonbForOtherFilters() {
    assertThat(
            SparkFilterUtils.isTopLevelFieldHandled(
                false,
                EqualTo.apply("field", new Object[] {"Json", "test"}),
                ImmutableMap.of("field", structFieldWithJson())))
        .isEqualTo(true);
  }

  @Test
  public void testNumericAndNullFilters() {
    RenderResult eq =
        SparkFilterUtils.compileFilter(EqualTo.apply("foo", 1), isPostgreSql, EMPTY_FIELDS, null);
    assertThat(eq.getSql()).isEqualTo(parseQuotedSplitter("{0}foo{0} = " + param(1)));
    assertThat(toStatement(eq).getParameters().get("p1")).isEqualTo(Value.int64(1));

    RenderResult eqNullSafe =
        SparkFilterUtils.compileFilter(
            EqualNullSafe.apply("foo", 1), isPostgreSql, EMPTY_FIELDS, null);
    assertThat(eqNullSafe.getSql())
        .isEqualTo(
            parseQuotedSplitter(
                "(({0}foo{0} IS NULL AND "
                    + param(1)
                    + " IS NULL) OR ({0}foo{0} IS NOT NULL AND "
                    + param(1)
                    + " IS NOT NULL AND {0}foo{0} = "
                    + param(1)
                    + "))"));
    assertThat(toStatement(eqNullSafe).getParameters().get("p1")).isEqualTo(Value.int64(1));

    RenderResult gt =
        SparkFilterUtils.compileFilter(
            GreaterThan.apply("foo", 2), isPostgreSql, EMPTY_FIELDS, null);
    assertThat(gt.getSql()).isEqualTo(parseQuotedSplitter("{0}foo{0} > " + param(1)));
    assertThat(toStatement(gt).getParameters().get("p1")).isEqualTo(Value.int64(2));

    RenderResult gte =
        SparkFilterUtils.compileFilter(
            GreaterThanOrEqual.apply("foo", 3), isPostgreSql, EMPTY_FIELDS, null);
    assertThat(gte.getSql()).isEqualTo(parseQuotedSplitter("{0}foo{0} >= " + param(1)));
    assertThat(toStatement(gte).getParameters().get("p1")).isEqualTo(Value.int64(3));

    RenderResult lt =
        SparkFilterUtils.compileFilter(LessThan.apply("foo", 4), isPostgreSql, EMPTY_FIELDS, null);
    assertThat(lt.getSql()).isEqualTo(parseQuotedSplitter("{0}foo{0} < " + param(1)));
    assertThat(toStatement(lt).getParameters().get("p1")).isEqualTo(Value.int64(4));

    RenderResult lte =
        SparkFilterUtils.compileFilter(
            LessThanOrEqual.apply("foo", 5), isPostgreSql, EMPTY_FIELDS, null);
    assertThat(lte.getSql()).isEqualTo(parseQuotedSplitter("{0}foo{0} <= " + param(1)));
    assertThat(toStatement(lte).getParameters().get("p1")).isEqualTo(Value.int64(5));

    RenderResult in =
        SparkFilterUtils.compileFilter(
            In.apply("foo", new Object[] {6, 7, 8}), isPostgreSql, EMPTY_FIELDS, null);
    assertThat(in.getSql())
        .isEqualTo(
            parseQuotedSplitter(
                "{0}foo{0} IN (" + param(1) + ", " + param(2) + ", " + param(3) + ")"));
    Statement inStmt = toStatement(in);
    assertThat(inStmt.getParameters().get("p1")).isEqualTo(Value.int64(6));
    assertThat(inStmt.getParameters().get("p2")).isEqualTo(Value.int64(7));
    assertThat(inStmt.getParameters().get("p3")).isEqualTo(Value.int64(8));

    assertThat(
            SparkFilterUtils.compileFilter(IsNull.apply("foo"), isPostgreSql, EMPTY_FIELDS, null)
                .getSql())
        .isEqualTo(parseQuotedSplitter("{0}foo{0} IS NULL"));
    assertThat(
            SparkFilterUtils.compileFilter(IsNotNull.apply("foo"), isPostgreSql, EMPTY_FIELDS, null)
                .getSql())
        .isEqualTo(parseQuotedSplitter("{0}foo{0} IS NOT NULL"));
    assertThat(
            SparkFilterUtils.compileFilter(
                    And.apply(IsNull.apply("foo"), IsNotNull.apply("bar")),
                    isPostgreSql,
                    EMPTY_FIELDS,
                    null)
                .getSql())
        .isEqualTo(parseQuotedSplitter("(({0}foo{0} IS NULL) AND ({0}bar{0} IS NOT NULL))"));
    assertThat(
            SparkFilterUtils.compileFilter(
                    Or.apply(IsNull.apply("foo"), IsNotNull.apply("bar")),
                    isPostgreSql,
                    EMPTY_FIELDS,
                    null)
                .getSql())
        .isEqualTo(parseQuotedSplitter("(({0}foo{0} IS NULL) OR ({0}bar{0} IS NOT NULL))"));
    assertThat(
            SparkFilterUtils.compileFilter(
                    Not.apply(IsNull.apply("foo")), isPostgreSql, EMPTY_FIELDS, null)
                .getSql())
        .isEqualTo(parseQuotedSplitter("(NOT ({0}foo{0} IS NULL))"));
  }

  @Test
  public void testDateFilters() throws ParseException {
    RenderResult result =
        SparkFilterUtils.compileFilter(
            In.apply(
                "datefield", new Object[] {Date.valueOf("2020-09-01"), Date.valueOf("2020-11-03")}),
            isPostgreSql,
            EMPTY_FIELDS,
            null);
    assertThat(result.getSql())
        .isEqualTo(parseQuotedSplitter("{0}datefield{0} IN (" + param(1) + ", " + param(2) + ")"));
    Statement stmt = toStatement(result);
    assertThat(stmt.getParameters().get("p1"))
        .isEqualTo(Value.date(com.google.cloud.Date.fromYearMonthDay(2020, 9, 1)));
    assertThat(stmt.getParameters().get("p2"))
        .isEqualTo(Value.date(com.google.cloud.Date.fromYearMonthDay(2020, 11, 3)));
  }

  @Test
  public void testBytesFilters() throws ParseException {
    byte[] bytes = "beefdead".getBytes(StandardCharsets.UTF_8);
    RenderResult result =
        SparkFilterUtils.compileFilter(
            In.apply("datefield", new Object[] {bytes}), isPostgreSql, EMPTY_FIELDS, null);
    assertThat(result.getSql())
        .isEqualTo(parseQuotedSplitter("{0}datefield{0} IN (" + param(1) + ")"));
    assertThat(toStatement(result).getParameters().get("p1"))
        .isEqualTo(Value.bytes(ByteArray.copyFrom(bytes)));
  }

  @Test
  public void testJsonFilters() {
    String jsonVal = "{\"tags\": [\"multi-cuisine\", \"open-seating\"], \"rating\": 4.5}";
    RenderResult result =
        SparkFilterUtils.compileFilter(
            In.apply("field", new Object[] {jsonVal}),
            isPostgreSql,
            ImmutableMap.of("field", structFieldWithJson()),
            null);
    if (isPostgreSql) {
      assertThat(result.getSql()).isEqualTo("CAST(\"field\" AS VARCHAR) IN ($1)");
    } else {
      assertThat(result.getSql()).isEqualTo("TO_JSON_STRING(`field`) IN (@p1)");
    }
    assertThat(toStatement(result).getParameters().get("p1")).isEqualTo(Value.string(jsonVal));
  }

  private StructField structFieldWithJson() {
    MetadataBuilder jsonMetaBuilder = new MetadataBuilder();
    jsonMetaBuilder.putString(SpannerUtils.COLUMN_TYPE, isPostgreSql ? "jsonb" : "json");
    return new StructField("field", DataTypes.StringType, true, jsonMetaBuilder.build());
  }

  @Test
  public void testDecimalFilters() throws ParseException {
    BigDecimal bd = new BigDecimal("1e20");
    RenderResult result =
        SparkFilterUtils.compileFilter(
            In.apply("datefield", new Object[] {bd}), isPostgreSql, EMPTY_FIELDS, null);
    assertThat(result.getSql())
        .isEqualTo(parseQuotedSplitter("{0}datefield{0} IN (" + param(1) + ")"));
    assertThat(toStatement(result).getParameters().get("p1"))
        .isEqualTo(isPostgreSql ? Value.pgNumeric(bd.toPlainString()) : Value.numeric(bd));
  }

  @Test
  public void testDateFilters_java8Time() {
    RenderResult result =
        SparkFilterUtils.compileFilter(
            In.apply(
                "datefield", new Object[] {LocalDate.of(2020, 9, 1), LocalDate.of(2020, 11, 3)}),
            isPostgreSql,
            EMPTY_FIELDS,
            null);
    assertThat(result.getSql())
        .isEqualTo(parseQuotedSplitter("{0}datefield{0} IN (" + param(1) + ", " + param(2) + ")"));
    Statement stmt = toStatement(result);
    assertThat(stmt.getParameters().get("p1"))
        .isEqualTo(Value.date(com.google.cloud.Date.fromYearMonthDay(2020, 9, 1)));
    assertThat(stmt.getParameters().get("p2"))
        .isEqualTo(Value.date(com.google.cloud.Date.fromYearMonthDay(2020, 11, 3)));
  }

  @Test
  public void testTimestampFilters() throws ParseException {
    Timestamp ts1 = Timestamp.valueOf("2008-12-25 15:30:00");
    Timestamp ts2 = Timestamp.valueOf("2020-01-25 02:10:10");
    RenderResult result =
        SparkFilterUtils.compileFilter(
            In.apply("tsfield", new Object[] {ts1, ts2}), isPostgreSql, EMPTY_FIELDS, null);
    assertThat(result.getSql())
        .isEqualTo(parseQuotedSplitter("{0}tsfield{0} IN (" + param(1) + ", " + param(2) + ")"));
    Statement stmt = toStatement(result);
    assertThat(stmt.getParameters().get("p1"))
        .isEqualTo(
            Value.timestamp(
                com.google.cloud.Timestamp.ofTimeSecondsAndNanos(
                    ts1.toInstant().getEpochSecond(), ts1.toInstant().getNano())));
    assertThat(stmt.getParameters().get("p2"))
        .isEqualTo(
            Value.timestamp(
                com.google.cloud.Timestamp.ofTimeSecondsAndNanos(
                    ts2.toInstant().getEpochSecond(), ts2.toInstant().getNano())));
  }

  @Test
  public void testTimestampFilters_java8Time() {
    Instant ts1 = LocalDateTime.of(2008, 12, 25, 15, 30, 0).toInstant(ZoneOffset.UTC);
    Instant ts2 = LocalDateTime.of(2020, 1, 25, 2, 10, 10).toInstant(ZoneOffset.UTC);
    RenderResult result =
        SparkFilterUtils.compileFilter(
            In.apply("tsfield", new Object[] {ts1, ts2}), isPostgreSql, EMPTY_FIELDS, null);
    assertThat(result.getSql())
        .isEqualTo(parseQuotedSplitter("{0}tsfield{0} IN (" + param(1) + ", " + param(2) + ")"));
    Statement stmt = toStatement(result);
    assertThat(stmt.getParameters().get("p1"))
        .isEqualTo(
            Value.timestamp(
                com.google.cloud.Timestamp.ofTimeSecondsAndNanos(
                    ts1.getEpochSecond(), ts1.getNano())));
    assertThat(stmt.getParameters().get("p2"))
        .isEqualTo(
            Value.timestamp(
                com.google.cloud.Timestamp.ofTimeSecondsAndNanos(
                    ts2.getEpochSecond(), ts2.getNano())));
  }

  @Test
  public void testFiltersWithNestedOrAnd_1() {
    // original query
    // (c1 >= 500 or c1 <= 70 or c1 >= 900 or c3 <= 50) and
    // (c1 >= 100 or c1 <= 700  or c2 <= 900) and
    // (c1 >= 5000 or c1 <= 701)

    Filter part1 =
        Or.apply(
            Or.apply(GreaterThanOrEqual.apply("c1", 500), LessThanOrEqual.apply("c1", 70)),
            Or.apply(GreaterThanOrEqual.apply("c1", 900), LessThanOrEqual.apply("c3", 50)));

    Filter part2 =
        Or.apply(
            Or.apply(GreaterThanOrEqual.apply("c1", 100), LessThanOrEqual.apply("c1", 700)),
            LessThanOrEqual.apply("c2", 900));

    Filter part3 = Or.apply(GreaterThanOrEqual.apply("c1", 5000), LessThanOrEqual.apply("c1", 701));

    checkFilters(
        pushAllFilters,
        "",
        parseQuotedSplitter(
            "((((({0}c1{0} >= "
                + param(1)
                + ") OR ({0}c1{0} <= "
                + param(2)
                + "))) OR ((({0}c1{0} >= "
                + param(3)
                + ") OR ({0}c3{0} <= "
                + param(4)
                + ")))) AND (((({0}c1{0} >= "
                + param(5)
                + ") OR ({0}c1{0} <= "
                + param(6)
                + "))) OR ({0}c2{0} <= "
                + param(7)
                + ")) AND (({0}c1{0} >= "
                + param(8)
                + ") OR ({0}c1{0} <= "
                + param(9)
                + ")))"),
        Optional.empty(),
        part1,
        part2,
        part3);
  }

  @Test
  public void testFiltersWithNestedOrAnd_2() {
    // original query
    // (c1 >= 500 and c2 <= 300) or (c1 <= 800 and c3 >= 230)

    Filter filter =
        Or.apply(
            And.apply(GreaterThanOrEqual.apply("c1", 500), LessThanOrEqual.apply("c2", 300)),
            And.apply(LessThanOrEqual.apply("c1", 800), GreaterThanOrEqual.apply("c3", 230)));

    checkFilters(
        pushAllFilters,
        "",
        parseQuotedSplitter(
            "((((({0}c1{0} >= "
                + param(1)
                + ") AND ({0}c2{0} <= "
                + param(2)
                + "))) OR ((({0}c1{0} <= "
                + param(3)
                + ") AND ({0}c3{0} >= "
                + param(4)
                + ")))))"),
        Optional.empty(),
        filter);
  }

  @Test
  public void testFiltersWithNestedOrAnd_3() {
    // original query
    // (((c1 >= 500 or c1 <= 70) and
    // (c1 >= 900 or (c3 <= 50 and (c2 >= 20 or c3 > 200))))) and
    // (((c1 >= 5000 or c1 <= 701) and (c2 >= 150 or c3 >= 100)) or
    // ((c1 >= 50 or c1 <= 71) and (c2 >= 15 or c3 >= 10)))

    Filter part1 = Or.apply(GreaterThanOrEqual.apply("c1", 500), LessThanOrEqual.apply("c1", 70));

    Filter part2 =
        Or.apply(
            GreaterThanOrEqual.apply("c1", 900),
            And.apply(
                LessThanOrEqual.apply("c3", 50),
                Or.apply(GreaterThanOrEqual.apply("c2", 20), GreaterThan.apply("c3", 200))));

    Filter part3 =
        Or.apply(
            And.apply(
                Or.apply(GreaterThanOrEqual.apply("c1", 5000), LessThanOrEqual.apply("c1", 701)),
                Or.apply(GreaterThanOrEqual.apply("c2", 150), GreaterThanOrEqual.apply("c3", 100))),
            And.apply(
                Or.apply(GreaterThanOrEqual.apply("c1", 50), LessThanOrEqual.apply("c1", 71)),
                Or.apply(GreaterThanOrEqual.apply("c2", 15), GreaterThanOrEqual.apply("c3", 10))));

    checkFilters(
        pushAllFilters,
        "",
        parseQuotedSplitter(
            "((({0}c1{0} >= "
                + param(1)
                + ") OR ({0}c1{0} <= "
                + param(2)
                + ")) AND (({0}c1{0} >= "
                + param(3)
                + ") OR ((({0}c3{0} <= "
                + param(4)
                + ") AND ((({0}c2{0} >= "
                + param(5)
                + ") OR ({0}c3{0} > "
                + param(6)
                + ")))))) AND (((((({0}c1{0} >= "
                + param(7)
                + ") OR ({0}c1{0} <= "
                + param(8)
                + "))) AND ((({0}c2{0} >= "
                + param(9)
                + ") OR ({0}c3{0} >= "
                + param(10)
                + "))))) OR ((((({0}c1{0} >= "
                + param(11)
                + ") OR ({0}c1{0} <= "
                + param(12)
                + "))) AND ((({0}c2{0} >= "
                + param(13)
                + ") OR ({0}c3{0} >= "
                + param(14)
                + ")))))))"),
        Optional.empty(),
        part1,
        part2,
        part3);
  }

  @Test
  public void testEqualNullSafeCombinedWithAndCannotBypassConjunct() {
    RenderResult nullEqAndTenant =
        SparkFilterUtils.getCompiledFilter(
            pushAllFilters,
            Optional.empty(),
            isPostgreSql,
            EMPTY_FIELDS,
            EqualNullSafe.apply("deleted_at", null),
            EqualTo.apply("tenant_id", "tenant_A"));
    assertThat(nullEqAndTenant.getSql())
        .isEqualTo(
            parseQuotedSplitter(
                "(({0}deleted_at{0} IS NULL) AND {0}tenant_id{0} = " + param(1) + ")"));
    assertThat(toStatement(nullEqAndTenant).getParameters().get("p1"))
        .isEqualTo(Value.string("tenant_A"));

    RenderResult valEqAndTenant =
        SparkFilterUtils.getCompiledFilter(
            pushAllFilters,
            Optional.empty(),
            isPostgreSql,
            EMPTY_FIELDS,
            EqualNullSafe.apply("deleted_at", 1),
            EqualTo.apply("tenant_id", "tenant_A"));
    assertThat(valEqAndTenant.getSql())
        .isEqualTo(
            parseQuotedSplitter(
                "((({0}deleted_at{0} IS NULL AND "
                    + param(1)
                    + " IS NULL) OR ({0}deleted_at{0} IS NOT NULL AND "
                    + param(1)
                    + " IS NOT NULL AND {0}deleted_at{0} = "
                    + param(1)
                    + ")) AND {0}tenant_id{0} = "
                    + param(2)
                    + ")"));
    Statement stmt = toStatement(valEqAndTenant);
    assertThat(stmt.getParameters().get("p1")).isEqualTo(Value.int64(1));
    assertThat(stmt.getParameters().get("p2")).isEqualTo(Value.string("tenant_A"));
  }

  @Test
  public void testDecimalFilterBoundsHandledPerDialect() {
    BigDecimal tooLargeForGsql = new BigDecimal("1E+29");
    BigDecimal tooFineScaleForGsql = new BigDecimal("0.0000000001");
    BigDecimal trailingZerosValid = new BigDecimal("1.2300000000");

    assertThat(
            SparkFilterUtils.isTopLevelFieldHandled(
                false, EqualTo.apply("dec", tooLargeForGsql), EMPTY_FIELDS, isPostgreSql))
        .isEqualTo(isPostgreSql);
    assertThat(
            SparkFilterUtils.isTopLevelFieldHandled(
                false, EqualTo.apply("dec", tooFineScaleForGsql), EMPTY_FIELDS, isPostgreSql))
        .isEqualTo(isPostgreSql);
    assertThat(
            SparkFilterUtils.isTopLevelFieldHandled(
                false, EqualTo.apply("dec", trailingZerosValid), EMPTY_FIELDS, isPostgreSql))
        .isTrue();
  }

  @Test
  public void testEmptyInCompilesToFalse() {
    RenderResult emptyIn =
        SparkFilterUtils.compileFilter(
            In.apply("foo", new Object[0]), isPostgreSql, EMPTY_FIELDS, null);
    assertThat(emptyIn.getSql()).isEqualTo("FALSE");
    assertThat(emptyIn.getBindings()).isEmpty();
  }

  @Test
  public void testUuidFiltersValidation() {
    MetadataBuilder uuidMeta = new MetadataBuilder();
    uuidMeta.putString(SpannerUtils.COLUMN_TYPE, "uuid");
    Map<String, StructField> uuidFields =
        ImmutableMap.of("id", new StructField("id", DataTypes.StringType, true, uuidMeta.build()));
    String validUuid = "550e8400-e29b-41d4-a716-446655440000";

    assertThat(
            SparkFilterUtils.isTopLevelFieldHandled(
                false, StringStartsWith.apply("id", "550e"), uuidFields, isPostgreSql))
        .isFalse();
    assertThat(
            SparkFilterUtils.isTopLevelFieldHandled(
                false, StringEndsWith.apply("id", "0000"), uuidFields, isPostgreSql))
        .isFalse();
    assertThat(
            SparkFilterUtils.isTopLevelFieldHandled(
                false, StringContains.apply("id", "e29b"), uuidFields, isPostgreSql))
        .isFalse();
    assertThat(
            SparkFilterUtils.isTopLevelFieldHandled(
                false, EqualTo.apply("id", "not-a-uuid"), uuidFields, isPostgreSql))
        .isFalse();
    assertThat(
            SparkFilterUtils.isTopLevelFieldHandled(
                false, EqualTo.apply("id", validUuid.toUpperCase()), uuidFields, isPostgreSql))
        .isFalse();
    assertThat(
            SparkFilterUtils.isTopLevelFieldHandled(
                false, EqualTo.apply("id", validUuid.substring(0, 35)), uuidFields, isPostgreSql))
        .isFalse();
    assertThat(
            SparkFilterUtils.isTopLevelFieldHandled(
                false,
                In.apply("id", new Object[] {validUuid, "BAD-UUID"}),
                uuidFields,
                isPostgreSql))
        .isFalse();

    assertThat(
            SparkFilterUtils.isTopLevelFieldHandled(
                false, EqualTo.apply("id", validUuid), uuidFields, isPostgreSql))
        .isTrue();
    assertThat(
            SparkFilterUtils.isTopLevelFieldHandled(
                false, EqualNullSafe.apply("id", null), uuidFields, isPostgreSql))
        .isTrue();
    assertThat(
            SparkFilterUtils.isTopLevelFieldHandled(
                false, In.apply("id", new Object[] {validUuid, null}), uuidFields, isPostgreSql))
        .isTrue();

    RenderResult compiledIn =
        SparkFilterUtils.compileFilter(
            In.apply("id", new Object[] {validUuid, null}), isPostgreSql, uuidFields, null);
    Statement stmt = toStatement(compiledIn);
    assertThat(stmt.getParameters().get("p1")).isEqualTo(Value.uuid(UUID.fromString(validUuid)));
    assertThat(stmt.getParameters().get("p2")).isEqualTo(Value.uuid(null));
  }

  @Test
  public void testCaseInsensitiveFieldLookupAndNullInTypeInference() {
    Map<String, StructField> mixedCaseFields =
        ImmutableMap.of(
            "IntCol", new StructField("IntCol", DataTypes.LongType, true, Metadata.empty()));
    RenderResult eqNull =
        SparkFilterUtils.compileFilter(
            EqualTo.apply("intcol", null), isPostgreSql, mixedCaseFields, null);
    assertThat(toStatement(eqNull).getParameters().get("p1")).isEqualTo(Value.int64(null));

    RenderResult inWithNullAndEmptyFields =
        SparkFilterUtils.compileFilter(
            In.apply("int_col", new Object[] {1L, null}), isPostgreSql, EMPTY_FIELDS, null);
    Statement inStmt = toStatement(inWithNullAndEmptyFields);
    assertThat(inStmt.getParameters().get("p1")).isEqualTo(Value.int64(1L));
    assertThat(inStmt.getParameters().get("p2")).isEqualTo(Value.int64(null));
  }
}
