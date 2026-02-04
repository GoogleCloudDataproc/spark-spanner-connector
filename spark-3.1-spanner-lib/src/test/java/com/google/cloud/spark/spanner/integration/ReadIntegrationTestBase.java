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

package com.google.cloud.spark.spanner.integration;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.Collator;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import scala.collection.JavaConverters;
import scala.collection.Seq;

public class ReadIntegrationTestBase extends SparkSpannerIntegrationTestBase {

  public Dataset<Row> readFromTable(String table) {
    return reader().option("table", table).load();
  }

  @Test
  public void testDataset_count() {
    Dataset<Row> df = readFromTable("compositeTable");
    assertThat(df.count()).isEqualTo(3);
  }

  @Test
  public void testNullInFilters() {
    Dataset<Row> df = readFromTable("nullsTable");
    assertThat(df.filter("A is null").count()).isEqualTo(2);
    assertThat(df.filter("B is null").count()).isEqualTo(3);
    assertThat(df.filter("C is null").count()).isEqualTo(5);
    assertThat(df.filter("D is null").count()).isEqualTo(5);
    assertThat(df.filter("E is null").count()).isEqualTo(6);
    assertThat(df.filter("F is null").count()).isEqualTo(6);
    assertThat(df.filter("G is null").count()).isEqualTo(4);
    assertThat(df.filter("H is null").count()).isEqualTo(3);
    assertThat(df.filter("I is null").count()).isEqualTo(4);
    assertThat(df.filter("J is null").count()).isEqualTo(2);
    assertThat(df.filter("K is null").count()).isEqualTo(1);
    assertThat(df.filter("M is null").count()).isEqualTo(4);
    assertThat(df.filter("N is null").count()).isEqualTo(4);
    assertThat(df.filter("O is null").count()).isEqualTo(2);
  }

  @Test
  public void testArrayFilters() {
    Dataset<Row> df = readFromTable("nullsTable");
    Row row = df.filter("A = array(null, 1234)").first();
    String results = toString(row);
    assertThat(results)
        .isEqualTo(
            "6 null 1234 null stringarray null null null null null null 2023-12-31 null 2023-09-23 12:11:09.0 null true null 1.0E-6 null {\"a\":1} null 123456.000000000");

    row = df.filter("B = array(null, 'stringarray')").first();
    results = toString(row);
    assertThat(results)
        .isEqualTo(
            "6 null 1234 null stringarray null null null null null null 2023-12-31 null 2023-09-23 12:11:09.0 null true null 1.0E-6 null {\"a\":1} null 123456.000000000");

    row = df.filter("H = array(null, date('2023-12-31'))").first();
    results = toString(row);
    assertThat(results)
        .isEqualTo(
            "6 null 1234 null stringarray null null null null null null 2023-12-31 null 2023-09-23 12:11:09.0 null true null 1.0E-6 null {\"a\":1} null 123456.000000000");

    assertThat(df.filter("I = array(null, timestamp('2023-09-23T12:11:09Z'))").count())
        .isEqualTo(2);

    row = df.filter("J = array(null, true)").first();
    results = toString(row);
    assertThat(results)
        .isEqualTo(
            "6 null 1234 null stringarray null null null null null null 2023-12-31 null 2023-09-23 12:11:09.0 null true null 1.0E-6 null {\"a\":1} null 123456.000000000");

    row = df.filter("K = array(null, double(0.000001))").first();
    results = toString(row);
    assertThat(results)
        .isEqualTo(
            "6 null 1234 null stringarray null null null null null null 2023-12-31 null 2023-09-23 12:11:09.0 null true null 1.0E-6 null {\"a\":1} null 123456.000000000");

    row = df.filter("M = array(null, binary('beefdead'))").first();
    results = toString(row);
    assertThat(results)
        .isEqualTo(
            "6 null 1234 null stringarray null null null null null null 2023-12-31 null 2023-09-23 12:11:09.0 null true null 1.0E-6 null {\"a\":1} null 123456.000000000");

    assertThat(df.filter("N = array(null, '{\"a\":1}')").count()).isEqualTo(2);

    row = df.filter("O = array(null, 123456)").first();
    results = toString(row);
    assertThat(results)
        .isEqualTo(
            "6 null 1234 null stringarray null null null null null null 2023-12-31 null 2023-09-23 12:11:09.0 null true null 1.0E-6 null {\"a\":1} null 123456.000000000");
  }

  @Test
  public void testEmptyArrayFilters() {
    Dataset<Row> df = readFromTable("nullsTable");
    Row row = df.filter("A = array()").first();
    String results = toString(row).trim();
    assertThat(results).isEqualTo("7 null null null null null");

    row = df.filter("B = array()").first();
    results = toString(row).trim();
    assertThat(results).isEqualTo("7 null null null null null");

    row = df.filter("H = array()").first();
    results = toString(row).trim();
    assertThat(results).isEqualTo("7 null null null null null");

    row = df.filter("I = array()").first();
    results = toString(row).trim();
    assertThat(results).isEqualTo("7 null null null null null");

    row = df.filter("J = array()").first();
    results = toString(row).trim();
    assertThat(results).isEqualTo("7 null null null null null");

    row = df.filter("K = array()").first();
    results = toString(row).trim();
    assertThat(results).isEqualTo("7 null null null null null");

    row = df.filter("M = array()").first();
    results = toString(row).trim();
    assertThat(results).isEqualTo("7 null null null null null");

    row = df.filter("N = array()").first();
    results = toString(row).trim();
    assertThat(results).isEqualTo("7 null null null null null");

    row = df.filter("O = array()").first();
    results = toString(row).trim();
    assertThat(results).isEqualTo("7 null null null null null");
  }

  @Test
  public void testNumericOutOfScopeFilter() {
    Dataset<Row> df = readFromTable("composite_table");
    Exception e =
        assertThrows(
            Exception.class,
            () -> {
              Row row =
                  df.filter(
                          "numericcol = 9999999999999999999999999999999999999999999.99999999999999999999999")
                      .first();
            });
    assertThat(e.getMessage()).contains("decimal can only support precision up to");
  }

  @Test
  public void testNumericFilter() {
    Dataset<Row> df = readFromTable("valueLimitsTable");
    Row row = df.filter("C = 99999999999999999999999999999.999999999").first();
    String results = toString(row);
    assertThat(results)
        .isEqualTo(
            "9223372036854775807 Infinity 99999999999999999999999999999.999999999 4000-12-30 2222-02-22 22:22:22.999999");

    row = df.filter("C > 99999999999999999999999999999").first();
    results = toString(row);
    assertThat(results)
        .isEqualTo(
            "9223372036854775807 Infinity 99999999999999999999999999999.999999999 4000-12-30 2222-02-22 22:22:22.999999");

    row =
        df.filter(
                "C in(99999999999999999999999999999.999999999, 99999999999999999999999999999.999999998)")
            .first();
    results = toString(row);
    assertThat(results)
        .isEqualTo(
            "9223372036854775807 Infinity 99999999999999999999999999999.999999999 4000-12-30 2222-02-22 22:22:22.999999");

    assertThat(df.filter("C is not null").count()).isEqualTo(4);
  }

  @Test
  public void testDoubleFilter() {
    Dataset<Row> df = readFromTable("valueLimitsTable");
    Row row = df.filter("isnan(B)").first();
    String results = toString(row);
    assertThat(results)
        .isEqualTo(
            "-9223372036854775808 NaN -99999999999999999999999999999.999999999 1699-12-31 9999-12-30 23:59:59.0");

    row = df.filter("B = double('inf')").first();
    results = toString(row);
    assertThat(results)
        .isEqualTo(
            "9223372036854775807 Infinity 99999999999999999999999999999.999999999 4000-12-30 2222-02-22 22:22:22.999999");

    row = df.filter("B = double('-inf')").first();
    results = toString(row);
    assertThat(results).isEqualTo("0 -Infinity 10.389000000 1900-12-30 2023-09-29 04:59:59.0");

    row = df.filter("B in (double(0.1), double(0.657818))").first();
    results = toString(row);
    assertThat(results).isEqualTo("1 0.657818 -10.389000000 2023-09-27 0001-01-03 00:00:01.0");

    assertThat(df.filter("B is not null").count()).isEqualTo(4);
  }

  @Test
  public void testBoolFilter() {
    Dataset<Row> df = readFromTable("compositeTable");
    Row row = df.filter("G = true").first();
    String results = toString(row);
    assertThat(results)
        .isEqualTo(
            "id1 10 100 991 567282 a b c foobar 2934.000000000 2023-01-01 2023-08-26 12:22:05.0 true 2023-01-02 2023-12-31 2023-08-26 12:11:10.0 2023-08-27 12:11:09.0 beefdead {\"a\":1,\"b\":2}");

    row = df.filter("G > false").first();
    results = toString(row);
    assertThat(results)
        .isEqualTo(
            "id1 10 100 991 567282 a b c foobar 2934.000000000 2023-01-01 2023-08-26 12:22:05.0 true 2023-01-02 2023-12-31 2023-08-26 12:11:10.0 2023-08-27 12:11:09.0 beefdead {\"a\":1,\"b\":2}");

    assertThat(df.filter("G is not null").count()).isEqualTo(2);

    assertThat(df.filter("G in(false, true)").count()).isEqualTo(2);
  }

  @Test
  public void testBytesFilter() {
    Dataset<Row> df = readFromTable("compositeTable");
    Row row = df.filter("J = 'deadbeef'").first();
    String results = toString(row);
    assertThat(results)
        .isEqualTo("id3 null null null null null null null null null 6465616462656566 null");

    row = df.filter("J like 'de%'").first();
    results = toString(row);
    assertThat(results)
        .isEqualTo("id3 null null null null null null null null null 6465616462656566 null");

    row = df.filter("J like '%ef'").first();
    results = toString(row);
    assertThat(results)
        .isEqualTo("id3 null null null null null null null null null 6465616462656566 null");

    row = df.filter("J like '%be%'").first();
    results = toString(row);
    assertThat(results)
        .isEqualTo("id3 null null null null null null null null null 6465616462656566 null");

    assertThat(df.filter("J is not null").count()).isEqualTo(3);

    row = df.filter("J in(binary('deadbeef'), binary('efef'))").first();
    results = toString(row);
    assertThat(results)
        .isEqualTo("id3 null null null null null null null null null 6465616462656566 null");
  }

  @Test
  public void testTimestampFilter() {
    Dataset<Row> df = readFromTable("compositeTable");
    Row row = df.filter("F = '2023-08-26 12:22:05'").first();
    String results = toString(row);
    assertThat(results)
        .isEqualTo(
            "id1 10 100 991 567282 a b c foobar 2934.000000000 2023-01-01 2023-08-26 12:22:05.0 true 2023-01-02 2023-12-31 2023-08-26 12:11:10.0 2023-08-27 12:11:09.0 beefdead {\"a\":1,\"b\":2}");

    row = df.filter("F = '2023-08-26 05:22:05 America/Los_Angeles'").first();
    results = toString(row);
    assertThat(results)
        .isEqualTo(
            "id1 10 100 991 567282 a b c foobar 2934.000000000 2023-01-01 2023-08-26 12:22:05.0 true 2023-01-02 2023-12-31 2023-08-26 12:11:10.0 2023-08-27 12:11:09.0 beefdead {\"a\":1,\"b\":2}");

    assertThat(df.filter("F > '2023-08-25 05:22:05 America/Los_Angeles'").count()).isEqualTo(2);

    assertThat(df.filter("F is not null").count()).isEqualTo(2);

    row =
        df.filter(
                "F in(timestamp('2023-08-26 05:22:05 America/Los_Angeles'), timestamp('2003-04-12 04:05:06 America/Los_Angeles'))")
            .first();
    results = toString(row);
    assertThat(results)
        .isEqualTo(
            "id1 10 100 991 567282 a b c foobar 2934.000000000 2023-01-01 2023-08-26 12:22:05.0 true 2023-01-02 2023-12-31 2023-08-26 12:11:10.0 2023-08-27 12:11:09.0 beefdead {\"a\":1,\"b\":2}");
  }

  @Test
  public void testDateFilter() {
    Dataset<Row> df = readFromTable("compositeTable");
    Row row = df.filter("E = '2023-01-01'").first();
    String results = toString(row);
    assertThat(results)
        .isEqualTo(
            "id1 10 100 991 567282 a b c foobar 2934.000000000 2023-01-01 2023-08-26 12:22:05.0 true 2023-01-02 2023-12-31 2023-08-26 12:11:10.0 2023-08-27 12:11:09.0 beefdead {\"a\":1,\"b\":2}");

    assertThat(df.filter("E > '2022-12-01'").count()).isEqualTo(2);

    assertThat(df.filter("E is not null").count()).isEqualTo(2);

    row = df.filter("E in(date('2023-01-01'), date('1700-01-01'))").first();
    results = toString(row);
    assertThat(results)
        .isEqualTo(
            "id1 10 100 991 567282 a b c foobar 2934.000000000 2023-01-01 2023-08-26 12:22:05.0 true 2023-01-02 2023-12-31 2023-08-26 12:11:10.0 2023-08-27 12:11:09.0 beefdead {\"a\":1,\"b\":2}");
  }

  @Test
  public void testReadFromCompositeTableJsonFilter() {
    Dataset<Row> df = readFromTable("compositeTable");
    assertThat(df.select("K").filter("K = '{\"a\":1,\"b\":2}'").count()).isEqualTo(1);
    Row row = df.select("K").filter("K = '{\"a\":1,\"b\":2}'").first();
    String result = String.valueOf(row.getString(0));
    assertThat(result).isEqualTo("{\"a\":1,\"b\":2}");

    assertThat(df.select("K").filter("K like '{\"a\":1,\"b\":2}'").count()).isEqualTo(1);
    row = df.select("K").filter("K = '{\"a\":1,\"b\":2}'").first();
    result = String.valueOf(row.getString(0));
    assertThat(result).isEqualTo("{\"a\":1,\"b\":2}");

    assertThat(df.select("K").filter("K is not null").count()).isEqualTo(2);

    assertThat(df.select("K").filter("K like '%a%'").count()).isEqualTo(1);
    row = df.select("K").filter("K = '{\"a\":1,\"b\":2}'").first();
    result = String.valueOf(row.getString(0));
    assertThat(result).isEqualTo("{\"a\":1,\"b\":2}");

    assertThat(df.select("K").filter("K in ('{\"a\":1,\"b\":2}')").count()).isEqualTo(1);
    row = df.select("K").filter("K = '{\"a\":1,\"b\":2}'").first();
    result = String.valueOf(row.getString(0));
    assertThat(result).isEqualTo("{\"a\":1,\"b\":2}");
  }

  @Test
  public void testReadFromCompositeTable() {
    Dataset<Row> df = readFromTable("compositeTable");
    Row row = df.filter("id = 'id1'").first();
    String results = toString(row);
    assertThat(results)
        .isEqualTo(
            "id1 10 100 991 567282 a b c foobar 2934.000000000 2023-01-01 2023-08-26 12:22:05.0 true 2023-01-02 2023-12-31 2023-08-26 12:11:10.0 2023-08-27 12:11:09.0 beefdead {\"a\":1,\"b\":2}");

    row = df.filter("id = 'id2'").first();
    results = toString(row);
    assertThat(results)
        .isEqualTo(
            "id2 20 200 2991 888885 A B C this one 93411.000000000 2023-09-23 2023-09-22 12:22:05.0 false 2023-09-02 2023-12-31 2023-09-22 12:11:10.0 2023-09-23 12:11:09.0 deadbeef {}");

    row = df.filter("id = 'id3'").first();
    results = toString(row);
    assertThat(results)
        .isEqualTo("id3 null null null null null null null null null 6465616462656566 null");
  }

  Timestamp asSparkTimestamp(String s) {
    return new Timestamp(ZonedDateTime.parse(s).toInstant().toEpochMilli());
  }

  @Test
  public void testDataset_schema() {
    Dataset<Row> df = readFromTable("compositeTable");
    StructType gotSchema = df.schema();
    StructType expectSchema =
        new StructType(
            Arrays.asList(
                    new StructField("id", DataTypes.StringType, false, null),
                    new StructField(
                        "A", DataTypes.createArrayType(DataTypes.LongType, true), true, null),
                    new StructField(
                        "B", DataTypes.createArrayType(DataTypes.StringType, true), true, null),
                    new StructField("C", DataTypes.StringType, true, null),
                    new StructField("D", DataTypes.createDecimalType(38, 9), true, null),
                    new StructField("E", DataTypes.DateType, true, null),
                    new StructField("F", DataTypes.TimestampType, true, null),
                    new StructField("G", DataTypes.BooleanType, true, null),
                    new StructField(
                        "H", DataTypes.createArrayType(DataTypes.DateType, true), true, null),
                    new StructField(
                        "I", DataTypes.createArrayType(DataTypes.TimestampType, true), true, null),
                    new StructField("J", DataTypes.BinaryType, true, null),
                    new StructField("K", DataTypes.StringType, true, null))
                .toArray(new StructField[0]));

    // For easy debugging, let's firstly compare the .treeString() values.
    // Object.equals fails for StructType with fields so firstly
    // compare lengths, then fieldNames then the treeString.
    assertThat(gotSchema.length()).isEqualTo(expectSchema.length());
    assertThat(gotSchema.fieldNames()).isEqualTo(expectSchema.fieldNames());
    assertThat(gotSchema.treeString()).isEqualTo(expectSchema.treeString());
  }

  @Test
  public void testDataset_sort() {
    Dataset<Row> df1 = readFromTable("simpleTable").sort("C");
    // 1. Sort by C, float64 values.
    List<Long> gotAsSortedByC = df1.select("A").as(Encoders.LONG()).collectAsList();
    List<Long> wantIdsSortedByC =
        Arrays.stream(new long[] {4, 9, 7, 8, 1, 2, 6, 3, 5}).boxed().collect(Collectors.toList());
    assertThat(gotAsSortedByC).isEqualTo(wantIdsSortedByC);
    // 1.5. Examine the actual C values and ensure they are ascending order.
    List<Double> gotCsSortedByC = df1.select("C").as(Encoders.DOUBLE()).collectAsList();
    List<Double> wantCsSortedByC =
        Arrays.asList(
            Double.NEGATIVE_INFINITY,
            -19999997.9,
            -0.1,
            +0.1,
            2.5,
            5.0,
            100000000017.100000000017,
            Double.POSITIVE_INFINITY,
            Double.NaN);
    assertThat(gotCsSortedByC).containsExactlyElementsIn(wantCsSortedByC).inOrder();

    // 2. Sort by Id, values should be in a different order.
    Dataset<Row> df2 = readFromTable("simpleTable").sort("A");
    List<Double> gotCsSortedByA = df2.select("C").as(Encoders.DOUBLE()).collectAsList();
    List<Double> wantCsSortedByA =
        Arrays.asList(
            2.5,
            5.0,
            Double.POSITIVE_INFINITY,
            Double.NEGATIVE_INFINITY,
            Double.NaN,
            100000000017.100000000017,
            -0.1,
            +0.1,
            -19999997.9);
    assertThat(gotCsSortedByA).containsExactlyElementsIn(wantCsSortedByA).inOrder();
  }

  @Test
  public void testDataset_where() {
    // 1. SELECT WHERE A < 8
    Dataset<Row> df = readFromTable("simpleTable");
    List<Long> gotAsLessThan8 = df.select("A").where("A < 8").as(Encoders.LONG()).collectAsList();
    List<Long> wantAsLessThan8 =
        Arrays.stream(new long[] {4, 7, 1, 2, 6, 3, 5}).boxed().collect(Collectors.toList());
    assertThat(gotAsLessThan8).containsExactlyElementsIn(wantAsLessThan8);

    // 2. SELECT WHERE A >= 8
    List<Long> gotAsGreaterThan7 =
        df.select("A").where("A > 7").as(Encoders.LONG()).collectAsList();
    List<Long> wantAsGreaterThan7 =
        Arrays.stream(new long[] {8, 9}).boxed().collect(Collectors.toList());
    assertThat(gotAsGreaterThan7).containsExactlyElementsIn(wantAsGreaterThan7);
  }

  @Test
  public void testDataset_takeAsList() {
    // 1. Firstly check that the count of rows unlimited is 9.
    assertThat(readFromTable("simpleTable").count()).isEqualTo(9);

    // 2. First 2 rows
    assertThat(readFromTable("simpleTable").takeAsList(2).size()).isEqualTo(2);

    // 3. Take the first 5 rows
    assertThat(readFromTable("simpleTable").takeAsList(5).size()).isEqualTo(5);
  }

  @Test
  public void testDataset_limit() {
    // 1. Firstly check that the count of rows unlimited is 9.
    assertThat(readFromTable("simpleTable").count()).isEqualTo(9);

    // 2. Limit to the first 2 rows.
    assertThat(readFromTable("simpleTable").limit(2).count()).isEqualTo(2);

    // 3. Limit to the first 5 rows.
    assertThat(readFromTable("simpleTable").limit(5).count()).isEqualTo(5);
  }

  private final Collator US_COLLATOR = Collator.getInstance(Locale.US);

  class customComparator implements Comparator<Object> {
    public int compare(Object a, Object b) {
      if (a == null && b == null) {
        return 0;
      }
      if (a == null) {
        return -1;
      }
      if (b == null) {
        return 1;
      }

      if ((a instanceof String) && (b instanceof String)) {
        return US_COLLATOR.compare(((String) a), ((String) b));
      }
      if ((a instanceof Long) && (b instanceof Long)) {
        return ((Long) a).compareTo(((Long) b));
      }
      if ((a instanceof BigDecimal) && (b instanceof BigDecimal)) {
        return ((BigDecimal) a).compareTo(((BigDecimal) b));
      }
      if ((a instanceof Date) && (b instanceof Date)) {
        return ((Date) a).compareTo(((Date) b));
      }
      if ((a instanceof Timestamp) && (b instanceof Timestamp)) {
        return ((Timestamp) a).compareTo(((Timestamp) b));
      }
      if ((a instanceof Boolean) && (b instanceof Boolean)) {
        return ((Boolean) a).compareTo(((Boolean) b));
      }
      if ((a instanceof Double) && (b instanceof Double)) {
        return ((Double) a).compareTo(((Double) b));
      }
      return -1;
    }
  }

  @Test
  public void testDataNullsTable() {
    Dataset<Row> df = readFromTable("nullsTable");
    List<String> actualRows =
        df.collectAsList().stream().map(row -> toString(row)).collect(Collectors.toList());
    assertEquals(actualRows.size(), 7);
    assertThat(actualRows)
        .containsExactlyElementsIn(
            ImmutableList.of(
                "1 null null null null null null true null 2023-09-22 null true null false 23.67 null null -99.371710000 null",
                "2 1 2 null null null 99.371710000 null null null 2022-10-01 null null null null true null 198.1827 null null null",
                "3 2 3 null a b FF null 😎🚨 null null 2023-09-23 12:11:09.0 false null null null -28888.8888 0.12 null null null null -55.700000000 9.300000000",
                "4 null 4 57 10 💡🚨 null b fg 🚨 55.700000000 2023-12-31 null false null null 2023-09-23 12:11:09.0 true true 0.71 null {\"a\":1} null 12.000000000",
                "5 null null null null null null null null null null null null null null",
                "6 null 1234 null stringarray null null null null null null 2023-12-31 null 2023-09-23 12:11:09.0 null true null 1.0E-6 null {\"a\":1} null 123456.000000000",
                "7 null null null null null"));
  }

  public void testValueLimits() {
    Dataset<Row> df = readFromTable("valueLimitsTable");
    assertThat(df.count()).isEqualTo(4);

    // 1. Test the limits of INT64.
    List<Long> gotAs = df.select("A").as(Encoders.LONG()).collectAsList();
    Collections.sort(gotAs);

    long[] lA = {Long.MIN_VALUE, Long.MAX_VALUE, 0, 1};
    List<Long> wantAs = Arrays.stream(lA).boxed().collect(Collectors.toList());
    Collections.sort(wantAs);
    assertThat(wantAs).containsExactlyElementsIn(gotAs);

    // 2. Test the limits of FLOAT64.
    List<Double> gotBs = df.select("B").as(Encoders.DOUBLE()).collectAsList();
    Collections.sort(gotBs);

    List<Double> wantBs =
        Arrays.asList(Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0.657818);
    Collections.sort(wantBs);
    assertThat(wantBs).containsExactlyElementsIn(gotBs);

    // 3. Test the limits of NUMERIC.
    List<BigDecimal> gotCs = df.select("C").as(Encoders.DECIMAL()).collectAsList();
    Collections.sort(gotCs);

    List<BigDecimal> wantCs =
        Arrays.asList(
            asSparkBigDecimal("-9.9999999999999999999999999999999999999E+28"),
            asSparkBigDecimal("9.9999999999999999999999999999999999999E+28"),
            asSparkBigDecimal("10.389"),
            asSparkBigDecimal("-10.389"));
    Collections.sort(wantCs);
    assertThat(wantCs).containsExactlyElementsIn(gotCs);
  }

  @Test
  public void testDateLimits() {
    Dataset<Row> df = readFromTable("valueLimitsTable");
    assertThat(df.count()).isEqualTo(4);

    // 1. Test the limits of INT64.
    List<Date> gotDs = df.select("D").as(Encoders.DATE()).collectAsList();
    Collections.sort(gotDs);

    List<Date> expectDs =
        Arrays.asList(
            Date.valueOf("1699-12-31"),
            Date.valueOf("4000-12-30"),
            Date.valueOf("1900-12-30"),
            Date.valueOf("2023-09-27"));
    Collections.sort(expectDs);

    assertThat(gotDs).containsExactlyElementsIn(expectDs);
  }

  @Test
  public void testTimestampLimits() {
    Dataset<Row> df = readFromTable("valueLimitsTable");
    assertThat(df.count()).isEqualTo(4);

    // 1. Test the limits of TIMESTAMP.
    List<Row> gotEs = df.select("E").collectAsList();
    List<String> actualList =
        gotEs.stream().map(row -> toString(row).trim()).collect(Collectors.toList());
    assertThat(actualList)
        .containsExactlyElementsIn(
            Arrays.asList(
                "0001-01-03 00:00:01.0",
                "2023-09-29 04:59:59.0",
                "2222-02-22 22:22:22.999999",
                "9999-12-30 23:59:59.0"));
    // Collections.sort(gotEs);

    // List<String> actualEStrs =
    //     gotEs.stream().map(timestamp -> timestamp.toString()).collect(toList());

    // List<String> expectEs =
    //     Arrays.asList(
    // );
    // assertThat(actualEStrs).containsExactlyElementsIn(expectEs);
  }

  public static String toString(Row row) {
    StructType types = row.schema();
    String rowStr = "";
    int i = 0;
    for (StructField field : types.fields()) {
      String fieldStr = toString(row, i, field.dataType());
      if (!"".equals(fieldStr)) {
        rowStr = rowStr.equals("") ? fieldStr : rowStr + " " + fieldStr;
      }
      i++;
    }
    return rowStr;
  }

  private static String toString(Row row, int index, DataType dataType) {
    if (row.isNullAt(index)) {
      return "null";
    }
    if (dataType == DataTypes.LongType) {
      return String.valueOf(row.getLong(index));
    } else if (dataType == DataTypes.StringType) {
      return String.valueOf(row.getString(index));
    } else if (dataType == DataTypes.BooleanType) {
      return String.valueOf(row.getBoolean(index));
    } else if (dataType == DataTypes.DoubleType) {
      return String.valueOf(row.getDouble(index));
    } else if (dataType == DataTypes.BinaryType) {
      return toStringForBytes(row.<byte[]>getAs(index));
    } else if (dataType == DataTypes.DateType) {
      return row.getDate(index).toString();
    } else if (dataType.equals(DataTypes.createDecimalType(38, 9))) {
      return row.getDecimal(index).toString();
    } else if (dataType == DataTypes.TimestampType) {
      return row.getTimestamp(index).toString();
    } else if (dataType == DataTypes.createArrayType(DataTypes.BinaryType)) {
      List<byte[]> byteList = row.getList(index);
      return byteList.stream()
          .map(bytes -> toStringForBytes(bytes))
          .collect(Collectors.joining(" "));
    } else if (dataType.equals(DataTypes.createArrayType(DataTypes.LongType))
        || dataType.equals(DataTypes.createArrayType(DataTypes.StringType))
        || dataType.equals(DataTypes.createArrayType(DataTypes.BooleanType))
        || dataType.equals(DataTypes.createArrayType(DataTypes.DoubleType))
        || dataType.equals(DataTypes.createArrayType(DataTypes.DateType))
        || dataType.equals(DataTypes.createArrayType(DataTypes.TimestampType))
        || dataType.equals(DataTypes.createArrayType(DataTypes.createDecimalType(38, 9)))) {
      return row.getList(index).stream()
          .map(obj -> String.valueOf(obj))
          .collect(Collectors.joining(" "));
    }

    return "";
  }

  Timestamp zdtToTs(String s, Optional<ZoneId> zoneId) {
    Instant dateTimeInstant = ZonedDateTime.parse(s).toInstant();
    ZonedDateTime dateTime;
    if (zoneId.isPresent()) {
      dateTime = dateTimeInstant.atZone(zoneId.get());
    } else {
      dateTime = dateTimeInstant.atZone(ZoneOffset.UTC);
    }

    return new Timestamp(dateTime.toInstant().toEpochMilli());
  }

  BigDecimal asSparkBigDecimal(String v) {
    return (new BigDecimal(v, new MathContext(38))).setScale(9);
  }

  private static <T> Seq<T> toSeq(List<T> list) {
    return JavaConverters.asScalaIteratorConverter(list.iterator()).asScala().toSeq();
  }

  private static byte[] stringToBytes(String str) {
    byte[] val = new byte[str.length() / 2];
    for (int i = 0; i < val.length; i++) {
      int index = i * 2;
      int j = Integer.parseInt(str.substring(index, index + 2), 16);
      val[i] = (byte) j;
    }
    return val;
  }

  private static String bytesToString(byte[] bytes) {
    return bytes == null ? "" : new String(bytes);
  }

  private void assertArrayEquals(List<byte[]> bytesList1, List<byte[]> bytesList2) {
    assertThat(bytesList1.size()).isEqualTo(bytesList2.size());

    for (int i = 0; i < bytesList1.size(); i++) {
      byte[] bytes1 = bytesList1.get(i);
      byte[] bytes2 = bytesList2.get(i);
      assertThat(bytesToString(bytes1)).isEqualTo(bytesToString(bytes2));
    }
  }

  private static String toStringForBytes(byte[] bytes) {
    if (bytes == null) {
      return "null";
    }
    String result = "";
    for (byte b : bytes) {
      result = result + String.format("%02x", b);
    }
    return result;
  }
}
