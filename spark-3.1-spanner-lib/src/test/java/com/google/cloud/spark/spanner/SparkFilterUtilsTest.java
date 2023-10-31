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
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.DataTypes;
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

  // The function parse the {0} into the quoted splitter and translate the '' into single quote '.
  private String parseQuotedSplitter(String s) {
    return MessageFormat.format(s, getQuotedSplitter());
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
        parseQuotedSplitter("(f>1) AND ({0}a{0} > 2)"),
        Optional.of("f>1"),
        GreaterThan.apply("a", 2));
  }

  @Test
  public void testNewFilterBehaviourNoFilterOption() {
    checkFilters(
        pushAllFilters,
        "",
        parseQuotedSplitter("({0}a{0} > 2)"),
        Optional.empty(),
        GreaterThan.apply("a", 2));
  }

  private void checkFilters(
      boolean pushAllFilters,
      String resultWithoutFilters,
      String resultWithFilters,
      Optional<String> configFilter,
      Filter... filters) {
    String result1 =
        SparkFilterUtils.getCompiledFilter(
            pushAllFilters, configFilter, isPostgreSql, EMPTY_FIELDS);
    assertThat(result1).isEqualTo(resultWithoutFilters);
    String result2 =
        SparkFilterUtils.getCompiledFilter(
            pushAllFilters, configFilter, isPostgreSql, EMPTY_FIELDS, filters);
    assertThat(result2).isEqualTo(resultWithFilters);
  }

  @Test
  public void testStringFilters() {
    assertThat(
            SparkFilterUtils.compileFilter(
                StringStartsWith.apply("foo", "bar"), isPostgreSql, EMPTY_FIELDS))
        .isEqualTo(parseQuotedSplitter("{0}foo{0} LIKE ''bar%''"));
    assertThat(
            SparkFilterUtils.compileFilter(
                StringEndsWith.apply("foo", "bar"), isPostgreSql, EMPTY_FIELDS))
        .isEqualTo(parseQuotedSplitter("{0}foo{0} LIKE ''%bar''"));
    assertThat(
            SparkFilterUtils.compileFilter(
                StringContains.apply("foo", "bar"), isPostgreSql, EMPTY_FIELDS))
        .isEqualTo(parseQuotedSplitter("{0}foo{0} LIKE ''%bar%''"));

    assertThat(
            SparkFilterUtils.compileFilter(
                StringStartsWith.apply("foo", "b'ar"), isPostgreSql, EMPTY_FIELDS))
        .isEqualTo(parseQuotedSplitter("{0}foo{0} LIKE ''b\\''ar%''"));
    assertThat(
            SparkFilterUtils.compileFilter(
                StringEndsWith.apply("foo", "b'ar"), isPostgreSql, EMPTY_FIELDS))
        .isEqualTo(parseQuotedSplitter("{0}foo{0} LIKE ''%b\\''ar''"));
    assertThat(
            SparkFilterUtils.compileFilter(
                StringContains.apply("foo", "b'ar"), isPostgreSql, EMPTY_FIELDS))
        .isEqualTo(parseQuotedSplitter("{0}foo{0} LIKE ''%b\\''ar%''"));
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
  public void testNumericAndNullFilters() {

    assertThat(SparkFilterUtils.compileFilter(EqualTo.apply("foo", 1), isPostgreSql, EMPTY_FIELDS))
        .isEqualTo(parseQuotedSplitter("{0}foo{0} = 1"));
    assertThat(
            SparkFilterUtils.compileFilter(
                EqualNullSafe.apply("foo", 1), isPostgreSql, EMPTY_FIELDS))
        .isEqualTo(
            parseQuotedSplitter(
                "{0}foo{0} IS NULL AND 1 IS NULL OR {0}foo{0} IS NOT NULL AND 1 IS NOT NULL AND {0}foo{0} = 1"));
    assertThat(
            SparkFilterUtils.compileFilter(GreaterThan.apply("foo", 2), isPostgreSql, EMPTY_FIELDS))
        .isEqualTo(parseQuotedSplitter("{0}foo{0} > 2"));
    assertThat(
            SparkFilterUtils.compileFilter(
                GreaterThanOrEqual.apply("foo", 3), isPostgreSql, EMPTY_FIELDS))
        .isEqualTo(parseQuotedSplitter("{0}foo{0} >= 3"));
    assertThat(SparkFilterUtils.compileFilter(LessThan.apply("foo", 4), isPostgreSql, EMPTY_FIELDS))
        .isEqualTo(parseQuotedSplitter("{0}foo{0} < 4"));
    assertThat(
            SparkFilterUtils.compileFilter(
                LessThanOrEqual.apply("foo", 5), isPostgreSql, EMPTY_FIELDS))
        .isEqualTo(parseQuotedSplitter("{0}foo{0} <= 5"));
    assertThat(
            SparkFilterUtils.compileFilter(
                In.apply("foo", new Object[] {6, 7, 8}), isPostgreSql, EMPTY_FIELDS))
        .isEqualTo(parseQuotedSplitter("{0}foo{0} IN (6, 7, 8)"));
    assertThat(SparkFilterUtils.compileFilter(IsNull.apply("foo"), isPostgreSql, EMPTY_FIELDS))
        .isEqualTo(parseQuotedSplitter("{0}foo{0} IS NULL"));
    assertThat(SparkFilterUtils.compileFilter(IsNotNull.apply("foo"), isPostgreSql, EMPTY_FIELDS))
        .isEqualTo(parseQuotedSplitter("{0}foo{0} IS NOT NULL"));
    assertThat(
            SparkFilterUtils.compileFilter(
                And.apply(IsNull.apply("foo"), IsNotNull.apply("bar")), isPostgreSql, EMPTY_FIELDS))
        .isEqualTo(parseQuotedSplitter("(({0}foo{0} IS NULL) AND ({0}bar{0} IS NOT NULL))"));
    assertThat(
            SparkFilterUtils.compileFilter(
                Or.apply(IsNull.apply("foo"), IsNotNull.apply("bar")), isPostgreSql, EMPTY_FIELDS))
        .isEqualTo(parseQuotedSplitter("(({0}foo{0} IS NULL) OR ({0}bar{0} IS NOT NULL))"));
    assertThat(
            SparkFilterUtils.compileFilter(
                Not.apply(IsNull.apply("foo")), isPostgreSql, EMPTY_FIELDS))
        .isEqualTo(parseQuotedSplitter("(NOT ({0}foo{0} IS NULL))"));
  }

  @Test
  public void testDateFilters() throws ParseException {
    assertThat(
            SparkFilterUtils.compileFilter(
                In.apply(
                    "datefield",
                    new Object[] {Date.valueOf("2020-09-01"), Date.valueOf("2020-11-03")}),
                isPostgreSql,
                EMPTY_FIELDS))
        .isEqualTo(
            parseQuotedSplitter("{0}datefield{0} IN (DATE ''2020-09-01'', DATE ''2020-11-03'')"));
  }

  @Test
  public void testBytesFilters() throws ParseException {
    if (isPostgreSql) {
      assertThat(
              SparkFilterUtils.compileFilter(
                  In.apply("datefield", new Object[] {"beefdead".getBytes(StandardCharsets.UTF_8)}),
                  isPostgreSql,
                  EMPTY_FIELDS))
          .isEqualTo(parseQuotedSplitter("{0}datefield{0} IN (''beefdead'')"));
    } else {
      assertThat(
              SparkFilterUtils.compileFilter(
                  In.apply("datefield", new Object[] {"beefdead".getBytes(StandardCharsets.UTF_8)}),
                  isPostgreSql,
                  EMPTY_FIELDS))
          .isEqualTo(parseQuotedSplitter("{0}datefield{0} IN (b''beefdead'')"));
    }
  }

  @Test
  public void testJsonFilters() {
    if (isPostgreSql) {
      assertThat(
              SparkFilterUtils.compileFilter(
                  In.apply(
                      "field",
                      new Object[] {
                        "{\"tags\": [\"multi-cuisine\", \"open-seating\"], \"rating\": 4.5}"
                      }),
                  isPostgreSql,
                  ImmutableMap.of("field", structFieldWithJson())))
          .isEqualTo(
              "CAST(\"field\" AS VARCHAR) IN ('{\"tags\": [\"multi-cuisine\", \"open-seating\"], \"rating\": 4.5}')");
    } else {
      assertThat(
              SparkFilterUtils.compileFilter(
                  In.apply(
                      "field",
                      new Object[] {
                        "{\"tags\": [\"multi-cuisine\", \"open-seating\"], \"rating\": 4.5}"
                      }),
                  isPostgreSql,
                  ImmutableMap.of("field", structFieldWithJson())))
          .isEqualTo(
              "TO_JSON_STRING(`field`) IN ('{\"tags\": [\"multi-cuisine\", \"open-seating\"], \"rating\": 4.5}')");
    }
  }

  private StructField structFieldWithJson() {
    MetadataBuilder jsonMetaBuilder = new MetadataBuilder();
    jsonMetaBuilder.putString(SpannerUtils.COLUMN_TYPE, isPostgreSql ? "jsonb" : "json");
    return new StructField("field", DataTypes.StringType, true, jsonMetaBuilder.build());
  }

  @Test
  public void testDecimalFilters() throws ParseException {
    assertThat(
            SparkFilterUtils.compileFilter(
                In.apply("datefield", new Object[] {new BigDecimal("1e20")}),
                isPostgreSql,
                EMPTY_FIELDS))
        .isEqualTo(parseQuotedSplitter("{0}datefield{0} IN (NUMERIC ''1E+20'')"));
  }

  @Test
  public void testDateFilters_java8Time() {
    assertThat(
            SparkFilterUtils.compileFilter(
                In.apply(
                    "datefield",
                    new Object[] {LocalDate.of(2020, 9, 1), LocalDate.of(2020, 11, 3)}),
                isPostgreSql,
                EMPTY_FIELDS))
        .isEqualTo(
            parseQuotedSplitter("{0}datefield{0} IN (DATE ''2020-09-01'', DATE ''2020-11-03'')"));
  }

  @Test
  public void testTimestampFilters() throws ParseException {
    Timestamp ts1 = Timestamp.valueOf("2008-12-25 15:30:00");
    Timestamp ts2 = Timestamp.valueOf("2020-01-25 02:10:10");
    assertThat(
            SparkFilterUtils.compileFilter(
                In.apply("tsfield", new Object[] {ts1, ts2}), isPostgreSql, EMPTY_FIELDS))
        .isEqualTo(
            parseQuotedSplitter(
                "{0}tsfield{0} IN (TIMESTAMP ''2008-12-25 15:30:00.0'', TIMESTAMP ''2020-01-25 02:10:10.0'')"));
  }

  @Test
  public void testTimestampFilters_java8Time() {
    Instant ts1 = LocalDateTime.of(2008, 12, 25, 15, 30, 0).toInstant(ZoneOffset.UTC);
    Instant ts2 = LocalDateTime.of(2020, 1, 25, 2, 10, 10).toInstant(ZoneOffset.UTC);
    assertThat(
            SparkFilterUtils.compileFilter(
                In.apply("tsfield", new Object[] {ts1, ts2}), isPostgreSql, EMPTY_FIELDS))
        .isEqualTo(
            parseQuotedSplitter(
                "{0}tsfield{0} IN (TIMESTAMP ''2008-12-25T15:30:00Z'', TIMESTAMP ''2020-01-25T02:10:10Z'')"));
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
            "((((({0}c1{0} >= 500) OR ({0}c1{0} <= 70))) OR ((({0}c1{0} >= 900) OR "
                + "({0}c3{0} <= 50)))) AND (((({0}c1{0} >= 100) OR ({0}c1{0} <= 700))) OR "
                + "({0}c2{0} <= 900)) AND (({0}c1{0} >= 5000) OR ({0}c1{0} <= 701)))"),
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
            "((((({0}c1{0} >= 500) AND ({0}c2{0} <= 300))) OR ((({0}c1{0} <= 800) AND ({0}c3{0} >= 230)))))"),
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
            "((({0}c1{0} >= 500) OR ({0}c1{0} <= 70)) AND (({0}c1{0} >= 900) OR "
                + "((({0}c3{0} <= 50) AND ((({0}c2{0} >= 20) OR ({0}c3{0} > 200)))))) AND "
                + "(((((({0}c1{0} >= 5000) OR ({0}c1{0} <= 701))) AND ((({0}c2{0} >= 150) OR "
                + "({0}c3{0} >= 100))))) OR ((((({0}c1{0} >= 50) OR ({0}c1{0} <= 71))) AND "
                + "((({0}c2{0} >= 15) OR ({0}c3{0} >= 10)))))))"),
        Optional.empty(),
        part1,
        part2,
        part3);
  }
}
