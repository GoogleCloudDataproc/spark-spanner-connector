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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.Test;

public class ReadIntegrationTestPg extends SparkSpannerIntegrationTestBase {
  private static Encoder<String> STRING_ENCODER = Encoders.STRING();
  private static int ROW_NUM_COMPOSITE_TABLE = 10;

  public Dataset<Row> readFromTable(String table) {
    Map<String, String> props = this.connectionProperties(true);
    return spark
        .read()
        .format("cloud-spanner")
        .option("viewsEnabled", true)
        .option("projectId", props.get("projectId"))
        .option("instanceId", props.get("instanceId"))
        .option("databaseId", props.get("databaseId"))
        .option("emulatorHost", props.get("emulatorHost"))
        .option("table", table)
        .load();
  }

  @Test
  public void testDataset_count() {
    Dataset<Row> df = readFromTable("integration_composite_table");
    assertThat(df.count()).isEqualTo(ROW_NUM_COMPOSITE_TABLE);
  }

  @Test
  public void testJsonFilter() {
    Dataset<Row> df = readFromTable("integration_composite_table");
    Row row = df.filter("jsoncol like '%tags%'").first();
    String results = toStringFromCompositeTable(row);

    assertThat(results)
        .isEqualTo(
            "2 charvcol textcol varcharcol true false 1 -1 0 1.0E-8 1.0E-8 beefdead 1999-01-08 123456.000000000 900000000000000000000000.000000000 2003-04-12 11:05:06.0 2003-04-12 12:05:06.0 {\"tags\": [\"multi-cuisine\", \"open-seating\"], \"rating\": 4.5}");

    row = df.filter("jsoncol is not null").first();
    results = toStringFromCompositeTable(row);
    assertThat(results)
        .isEqualTo(
            "2 charvcol textcol varcharcol true false 1 -1 0 1.0E-8 1.0E-8 beefdead 1999-01-08 123456.000000000 900000000000000000000000.000000000 2003-04-12 11:05:06.0 2003-04-12 12:05:06.0 {\"tags\": [\"multi-cuisine\", \"open-seating\"], \"rating\": 4.5}");

    row =
        df.filter("jsoncol = '{\"tags\": [\"multi-cuisine\", \"open-seating\"], \"rating\": 4.5}'")
            .first();
    results = toStringFromCompositeTable(row);
    assertThat(results)
        .isEqualTo(
            "2 charvcol textcol varcharcol true false 1 -1 0 1.0E-8 1.0E-8 beefdead 1999-01-08 123456.000000000 900000000000000000000000.000000000 2003-04-12 11:05:06.0 2003-04-12 12:05:06.0 {\"tags\": [\"multi-cuisine\", \"open-seating\"], \"rating\": 4.5}");

    row =
        df.filter(
                "jsoncol in('{\"tags\": [\"multi-cuisine\", \"open-seating\"], \"rating\": 4.5}', 'tags')")
            .first();
    results = toStringFromCompositeTable(row);
    assertThat(results)
        .isEqualTo(
            "2 charvcol textcol varcharcol true false 1 -1 0 1.0E-8 1.0E-8 beefdead 1999-01-08 123456.000000000 900000000000000000000000.000000000 2003-04-12 11:05:06.0 2003-04-12 12:05:06.0 {\"tags\": [\"multi-cuisine\", \"open-seating\"], \"rating\": 4.5}");
  }

  @Test
  public void testNullInFilters() {
    Dataset<Row> df = readFromTable("integration_composite_table");
    assertThat(df.filter("charvcol is null").count()).isEqualTo(ROW_NUM_COMPOSITE_TABLE - 1);
    assertThat(df.filter("boolcol is null").count()).isEqualTo(ROW_NUM_COMPOSITE_TABLE - 1);
    assertThat(df.filter("bigintcol is null").count()).isEqualTo(ROW_NUM_COMPOSITE_TABLE - 2);
    assertThat(df.filter("doublecol is null").count()).isEqualTo(ROW_NUM_COMPOSITE_TABLE - 3);
    assertThat(df.filter("bytecol is null").count()).isEqualTo(ROW_NUM_COMPOSITE_TABLE - 1);
    assertThat(df.filter("datecol is null").count()).isEqualTo(ROW_NUM_COMPOSITE_TABLE - 3);
    assertThat(df.filter("numericcol is null").count()).isEqualTo(ROW_NUM_COMPOSITE_TABLE - 3);
    assertThat(df.filter("timewithzonecol is null").count()).isEqualTo(ROW_NUM_COMPOSITE_TABLE - 2);
    assertThat(df.filter("jsoncol is null").count()).isEqualTo(ROW_NUM_COMPOSITE_TABLE - 1);

    df = readFromTable("array_table");
    assertThat(df.filter("charvarray is null").count()).isEqualTo(1);
    assertThat(df.filter("boolarray is null").count()).isEqualTo(1);
    assertThat(df.filter("bigintarray is null").count()).isEqualTo(1);
    assertThat(df.filter("doublearray is null").count()).isEqualTo(1);
    assertThat(df.filter("bytearray is null").count()).isEqualTo(1);
    assertThat(df.filter("datearray is null").count()).isEqualTo(1);
    assertThat(df.filter("numericarray is null").count()).isEqualTo(1);
    assertThat(df.filter("timestamparray is null").count()).isEqualTo(1);
    assertThat(df.filter("jsonarray is null").count()).isEqualTo(1);
  }

  @Test
  public void testArrayFilters() {
    Dataset<Row> df = readFromTable("array_table");
    Row row = df.filter("charvarray = array(null, 'charvarray')").first();
    String results = toStringFromArrayTable(row);
    assertThat(results)
        .isEqualTo(
            "2 null charvarray null true null 1024 null 1.0E-8 null beefdead null 1999-01-08 null 123450.000000000 null 2003-04-12 11:05:06.0 null {\"tags\": [\"multi-cuisine\", \"open-seating\"], \"rating\": 4.5}");

    row = df.filter("boolarray = array(null, true)").first();
    results = toStringFromArrayTable(row);
    assertThat(results)
        .isEqualTo(
            "2 null charvarray null true null 1024 null 1.0E-8 null beefdead null 1999-01-08 null 123450.000000000 null 2003-04-12 11:05:06.0 null {\"tags\": [\"multi-cuisine\", \"open-seating\"], \"rating\": 4.5}");

    row = df.filter("bigintarray = array(null, 1024)").first();
    results = toStringFromArrayTable(row);
    assertThat(results)
        .isEqualTo(
            "2 null charvarray null true null 1024 null 1.0E-8 null beefdead null 1999-01-08 null 123450.000000000 null 2003-04-12 11:05:06.0 null {\"tags\": [\"multi-cuisine\", \"open-seating\"], \"rating\": 4.5}");

    row = df.filter("doublearray = array(null, double(0.00000001))").first();
    results = toStringFromArrayTable(row);
    assertThat(results)
        .isEqualTo(
            "2 null charvarray null true null 1024 null 1.0E-8 null beefdead null 1999-01-08 null 123450.000000000 null 2003-04-12 11:05:06.0 null {\"tags\": [\"multi-cuisine\", \"open-seating\"], \"rating\": 4.5}");

    row = df.filter("bytearray = array(null, binary('beefdead'))").first();
    results = toStringFromArrayTable(row);
    assertThat(results)
        .isEqualTo(
            "2 null charvarray null true null 1024 null 1.0E-8 null beefdead null 1999-01-08 null 123450.000000000 null 2003-04-12 11:05:06.0 null {\"tags\": [\"multi-cuisine\", \"open-seating\"], \"rating\": 4.5}");

    row = df.filter("datearray = array(null, date('1999-01-08'))").first();
    results = toStringFromArrayTable(row);
    assertThat(results)
        .isEqualTo(
            "2 null charvarray null true null 1024 null 1.0E-8 null beefdead null 1999-01-08 null 123450.000000000 null 2003-04-12 11:05:06.0 null {\"tags\": [\"multi-cuisine\", \"open-seating\"], \"rating\": 4.5}");

    row =
        df.filter(
                "numericarray = array(CAST(NULL AS DECIMAL(38,9)), CAST('1.2345e05' AS DECIMAL(38,9)))")
            .first();
    results = toStringFromArrayTable(row);
    assertThat(results)
        .isEqualTo(
            "2 null charvarray null true null 1024 null 1.0E-8 null beefdead null 1999-01-08 null 123450.000000000 null 2003-04-12 11:05:06.0 null {\"tags\": [\"multi-cuisine\", \"open-seating\"], \"rating\": 4.5}");

    row =
        df.filter(
                "timestamparray = array(null, timestamp('2003-04-12 04:05:06 America/Los_Angeles'))")
            .first();
    results = toStringFromArrayTable(row);
    assertThat(results)
        .isEqualTo(
            "2 null charvarray null true null 1024 null 1.0E-8 null beefdead null 1999-01-08 null 123450.000000000 null 2003-04-12 11:05:06.0 null {\"tags\": [\"multi-cuisine\", \"open-seating\"], \"rating\": 4.5}");

    row =
        df.filter(
                "jsonarray = array(null, '{\"tags\": [\"multi-cuisine\", \"open-seating\"], \"rating\": 4.5}')")
            .first();
    results = toStringFromArrayTable(row);
    assertThat(results)
        .isEqualTo(
            "2 null charvarray null true null 1024 null 1.0E-8 null beefdead null 1999-01-08 null 123450.000000000 null 2003-04-12 11:05:06.0 null {\"tags\": [\"multi-cuisine\", \"open-seating\"], \"rating\": 4.5}");
  }

  @Test
  public void testEmptyArrayFilters() {
    Dataset<Row> df = readFromTable("array_table");
    Row row = df.filter("charvarray = array()").first();
    String results = toStringFromArrayTable(row).trim();
    assertThat(results).isEqualTo("3");

    row = df.filter("boolarray = array()").first();
    results = toStringFromArrayTable(row).trim();
    assertThat(results).isEqualTo("3");

    row = df.filter("bigintarray = array()").first();
    results = toStringFromArrayTable(row).trim();
    assertThat(results).isEqualTo("3");

    row = df.filter("doublearray = array()").first();
    results = toStringFromArrayTable(row).trim();
    assertThat(results).isEqualTo("3");

    row = df.filter("bytearray = array()").first();
    results = toStringFromArrayTable(row).trim();
    assertThat(results).isEqualTo("3");

    row = df.filter("datearray = array()").first();
    results = toStringFromArrayTable(row).trim();
    assertThat(results).isEqualTo("3");

    row = df.filter("numericarray = array()").first();
    results = toStringFromArrayTable(row).trim();
    assertThat(results).isEqualTo("3");

    row = df.filter("timestamparray = array()").first();
    results = toStringFromArrayTable(row).trim();
    assertThat(results).isEqualTo("3");

    row = df.filter("jsonarray = array()").first();
    results = toStringFromArrayTable(row).trim();
    assertThat(results).isEqualTo("3");
  }

  @Test
  public void testNumericOutOfScopeFilter() {
    Dataset<Row> df = readFromTable("integration_composite_table");
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
    Dataset<Row> df = readFromTable("integration_composite_table");
    Row row = df.filter("numericcol = 99999999999999999999999999999.999999999").first();
    String results = toStringFromCompositeTable(row);
    assertThat(results)
        .isEqualTo(
            "7 null null null null null null null null null null null null 99999999999999999999999999999.999999999 -99999999999999999999999999999.999999999 null null null");

    /**
     * There's an issue when comparing numeric columns. Disabling the test first. row = df.filter(
     * "numericcol in(99999999999999999999999999999.999999999,
     * 99999999999999999999999999999.999999998)") .first(); results =
     * toStringFromCompositeTable(row); assertThat(results) .isEqualTo( "7 null null null null null
     * null null null null null null null 99999999999999999999999999999.999999999
     * -99999999999999999999999999999.999999999 null null null");
     */
    assertThat(df.filter("numericcol is not null").count()).isEqualTo(3);
  }

  @Test
  public void testDoubleFilter() {
    Dataset<Row> df = readFromTable("integration_composite_table");
    Row row = df.filter("isnan(doublecol)").first();
    String results = toStringFromCompositeTable(row);
    assertThat(results)
        .isEqualTo(
            "4 null null null null null null null null NaN NaN null null null null null null null");

    row = df.filter("doublecol = double('inf')").first();
    results = toStringFromCompositeTable(row);
    assertThat(results)
        .isEqualTo(
            "10 null null null null null null null null Infinity -Infinity null null null null null null null");

    row = df.filter("doublecol in (double(0.00000001), double(0.00000002))").first();
    results = toStringFromCompositeTable(row);
    assertThat(results)
        .isEqualTo(
            "2 charvcol textcol varcharcol true false 1 -1 0 1.0E-8 1.0E-8 beefdead 1999-01-08 123456.000000000 900000000000000000000000.000000000 2003-04-12 11:05:06.0 2003-04-12 12:05:06.0 {\"tags\": [\"multi-cuisine\", \"open-seating\"], \"rating\": 4.5}");

    assertThat(df.filter("doublecol is not null").count()).isEqualTo(3);
  }

  @Test
  public void testBoolFilter() {
    Dataset<Row> df = readFromTable("integration_composite_table");
    Row row = df.filter("boolcol = true").first();
    String results = toStringFromCompositeTable(row);
    assertThat(results)
        .isEqualTo(
            "2 charvcol textcol varcharcol true false 1 -1 0 1.0E-8 1.0E-8 beefdead 1999-01-08 123456.000000000 900000000000000000000000.000000000 2003-04-12 11:05:06.0 2003-04-12 12:05:06.0 {\"tags\": [\"multi-cuisine\", \"open-seating\"], \"rating\": 4.5}");

    row = df.filter("boolcol >= false").first();
    results = toStringFromCompositeTable(row);
    assertThat(results)
        .isEqualTo(
            "2 charvcol textcol varcharcol true false 1 -1 0 1.0E-8 1.0E-8 beefdead 1999-01-08 123456.000000000 900000000000000000000000.000000000 2003-04-12 11:05:06.0 2003-04-12 12:05:06.0 {\"tags\": [\"multi-cuisine\", \"open-seating\"], \"rating\": 4.5}");

    row = df.filter("boolcol is not null").first();
    results = toStringFromCompositeTable(row);
    assertThat(results)
        .isEqualTo(
            "2 charvcol textcol varcharcol true false 1 -1 0 1.0E-8 1.0E-8 beefdead 1999-01-08 123456.000000000 900000000000000000000000.000000000 2003-04-12 11:05:06.0 2003-04-12 12:05:06.0 {\"tags\": [\"multi-cuisine\", \"open-seating\"], \"rating\": 4.5}");

    row = df.filter("boolcol in(true, false)").first();
    results = toStringFromCompositeTable(row);
    assertThat(results)
        .isEqualTo(
            "2 charvcol textcol varcharcol true false 1 -1 0 1.0E-8 1.0E-8 beefdead 1999-01-08 123456.000000000 900000000000000000000000.000000000 2003-04-12 11:05:06.0 2003-04-12 12:05:06.0 {\"tags\": [\"multi-cuisine\", \"open-seating\"], \"rating\": 4.5}");
  }

  @Test
  public void testBytesFilter() {
    Dataset<Row> df = readFromTable("integration_composite_table");
    Row row = df.filter("bytecol = 'beefdead'").first();
    String results = toStringFromCompositeTable(row);
    assertThat(results)
        .isEqualTo(
            "2 charvcol textcol varcharcol true false 1 -1 0 1.0E-8 1.0E-8 beefdead 1999-01-08 123456.000000000 900000000000000000000000.000000000 2003-04-12 11:05:06.0 2003-04-12 12:05:06.0 {\"tags\": [\"multi-cuisine\", \"open-seating\"], \"rating\": 4.5}");

    row = df.filter("bytecol like 'be%'").first();
    results = toStringFromCompositeTable(row);
    assertThat(results)
        .isEqualTo(
            "2 charvcol textcol varcharcol true false 1 -1 0 1.0E-8 1.0E-8 beefdead 1999-01-08 123456.000000000 900000000000000000000000.000000000 2003-04-12 11:05:06.0 2003-04-12 12:05:06.0 {\"tags\": [\"multi-cuisine\", \"open-seating\"], \"rating\": 4.5}");

    row = df.filter("bytecol like '%e%'").first();
    results = toStringFromCompositeTable(row);
    assertThat(results)
        .isEqualTo(
            "2 charvcol textcol varcharcol true false 1 -1 0 1.0E-8 1.0E-8 beefdead 1999-01-08 123456.000000000 900000000000000000000000.000000000 2003-04-12 11:05:06.0 2003-04-12 12:05:06.0 {\"tags\": [\"multi-cuisine\", \"open-seating\"], \"rating\": 4.5}");

    row = df.filter("bytecol is not null").first();
    results = toStringFromCompositeTable(row);
    assertThat(results)
        .isEqualTo(
            "2 charvcol textcol varcharcol true false 1 -1 0 1.0E-8 1.0E-8 beefdead 1999-01-08 123456.000000000 900000000000000000000000.000000000 2003-04-12 11:05:06.0 2003-04-12 12:05:06.0 {\"tags\": [\"multi-cuisine\", \"open-seating\"], \"rating\": 4.5}");

    row = df.filter("bytecol IN (binary('beefdead'), binary('deadbeef'))").first();
    results = toStringFromCompositeTable(row);
    assertThat(results)
        .isEqualTo(
            "2 charvcol textcol varcharcol true false 1 -1 0 1.0E-8 1.0E-8 beefdead 1999-01-08 123456.000000000 900000000000000000000000.000000000 2003-04-12 11:05:06.0 2003-04-12 12:05:06.0 {\"tags\": [\"multi-cuisine\", \"open-seating\"], \"rating\": 4.5}");
  }

  @Test
  public void testTimestampFilter() {
    Dataset<Row> df = readFromTable("integration_composite_table");
    Row row = df.filter("timewithzonecol = '2003-04-12 04:05:06 America/Los_Angeles'").first();
    String results = toStringFromCompositeTable(row);
    assertThat(results)
        .isEqualTo(
            "2 charvcol textcol varcharcol true false 1 -1 0 1.0E-8 1.0E-8 beefdead 1999-01-08 123456.000000000 900000000000000000000000.000000000 2003-04-12 11:05:06.0 2003-04-12 12:05:06.0 {\"tags\": [\"multi-cuisine\", \"open-seating\"], \"rating\": 4.5}");

    row = df.filter("timewithzonecol = '2003-04-12 11:05:06'").first();
    results = toStringFromCompositeTable(row);
    assertThat(results)
        .isEqualTo(
            "2 charvcol textcol varcharcol true false 1 -1 0 1.0E-8 1.0E-8 beefdead 1999-01-08 123456.000000000 900000000000000000000000.000000000 2003-04-12 11:05:06.0 2003-04-12 12:05:06.0 {\"tags\": [\"multi-cuisine\", \"open-seating\"], \"rating\": 4.5}");

    row = df.filter("timewithzonecol > '2003-04-11 04:05:06 America/Los_Angeles'").first();
    results = toStringFromCompositeTable(row);
    assertThat(results)
        .isEqualTo(
            "2 charvcol textcol varcharcol true false 1 -1 0 1.0E-8 1.0E-8 beefdead 1999-01-08 123456.000000000 900000000000000000000000.000000000 2003-04-12 11:05:06.0 2003-04-12 12:05:06.0 {\"tags\": [\"multi-cuisine\", \"open-seating\"], \"rating\": 4.5}");

    df = readFromTable("integration_composite_table");
    row = df.filter("timewithzonecol = '0001-01-01 23:00:00 America/Los_Angeles'").first();
    results = toStringFromCompositeTable(row);
    assertThat(results)
        .isEqualTo(
            "8 null null null null null null null null null null null null null null 0001-01-02 06:52:58.0 9999-12-30 09:00:00.0 null");

    row =
        df.filter(
                "timewithzonecol IN (timestamp('2003-04-12 04:05:06 America/Los_Angeles'), timestamp('2003-04-13 04:05:06 America/Los_Angeles'))")
            .first();
    results = toStringFromCompositeTable(row);
    assertThat(results)
        .isEqualTo(
            "2 charvcol textcol varcharcol true false 1 -1 0 1.0E-8 1.0E-8 beefdead 1999-01-08 123456.000000000 900000000000000000000000.000000000 2003-04-12 11:05:06.0 2003-04-12 12:05:06.0 {\"tags\": [\"multi-cuisine\", \"open-seating\"], \"rating\": 4.5}");

    assertThat(df.filter("timewithzonecol is not null").count()).isEqualTo(2);
  }

  @Test
  public void testDateFilter() {
    Dataset<Row> df = readFromTable("integration_composite_table");
    Row row = df.filter("datecol = '1999-01-08'").first();
    String results = toStringFromCompositeTable(row);
    assertThat(results)
        .isEqualTo(
            "2 charvcol textcol varcharcol true false 1 -1 0 1.0E-8 1.0E-8 beefdead 1999-01-08 123456.000000000 900000000000000000000000.000000000 2003-04-12 11:05:06.0 2003-04-12 12:05:06.0 {\"tags\": [\"multi-cuisine\", \"open-seating\"], \"rating\": 4.5}");

    row = df.filter("datecol > '1999-01-07'").first();
    results = toStringFromCompositeTable(row);
    assertThat(results)
        .isEqualTo(
            "2 charvcol textcol varcharcol true false 1 -1 0 1.0E-8 1.0E-8 beefdead 1999-01-08 123456.000000000 900000000000000000000000.000000000 2003-04-12 11:05:06.0 2003-04-12 12:05:06.0 {\"tags\": [\"multi-cuisine\", \"open-seating\"], \"rating\": 4.5}");

    // df = readFromTable("integration_composite_table");
    row = df.filter("datecol = '1700-01-01'").first();
    results = toStringFromCompositeTable(row);
    assertThat(results)
        .isEqualTo(
            "6 null null null null null null null null null null null 1700-01-01 null null null null null");

    row = df.filter("datecol in(date('1700-01-01'), date('1700-01-02'))").first();
    results = toStringFromCompositeTable(row);
    assertThat(results)
        .isEqualTo(
            "6 null null null null null null null null null null null 1700-01-01 null null null null null");

    assertThat(df.filter("datecol is not null").count()).isEqualTo(3);
  }

  @Test
  public void testReadNullFromCompositeTable() {
    Dataset<Row> df = readFromTable("integration_composite_table");
    Row row = df.filter("id = 1").first();

    for (int i = 1; i < row.length(); i++) {
      assertThat(row.isNullAt(i)).isEqualTo(true);
    }
  }

  @Test
  public void testReadValuesFromCompositeTable() {
    Dataset<Row> df = readFromTable("integration_composite_table");
    Row row = df.filter("id = 2").first();

    String results = toStringFromCompositeTable(row);
    assertThat(results)
        .isEqualTo(
            "2 charvcol textcol varcharcol true false 1 -1 0 1.0E-8 1.0E-8 beefdead 1999-01-08 123456.000000000 900000000000000000000000.000000000 2003-04-12 11:05:06.0 2003-04-12 12:05:06.0 {\"tags\": [\"multi-cuisine\", \"open-seating\"], \"rating\": 4.5}");
  }

  @Test
  public void testReadIntegerLimitFromCompositeTable() {
    Dataset<Row> df = readFromTable("integration_composite_table");
    Row row = df.filter("id = 3").first();

    String results = toStringFromCompositeTable(row);
    assertThat(results)
        .isEqualTo(
            "3 null null null null null 9223372036854775807 -9223372036854775808 0 null null null null null null null null null");
  }

  @Test
  public void testReadDoubleLimitFromCompositeTable() {
    Dataset<Row> df = readFromTable("integration_composite_table");
    Row row = df.filter("id = 4").first();

    String results = toStringFromCompositeTable(row);
    assertThat(results)
        .isEqualTo(
            "4 null null null null null null null null NaN NaN null null null null null null null");
  }

  @Test
  public void testReadDoubleInfFromCompositeTable() {
    Dataset<Row> df = readFromTable("integration_composite_table");
    Row row = df.filter("id = 10").first();

    String results = toStringFromCompositeTable(row);
    assertThat(results)
        .isEqualTo(
            "10 null null null null null null null null Infinity -Infinity null null null null null null null");
  }

  @Test
  public void testReadDateUpperLimitFromCompositeTable() {
    Dataset<Row> df = readFromTable("integration_composite_table");
    Row row = df.filter("id = 5").first();

    String results = toStringFromCompositeTable(row);
    assertThat(results)
        .isEqualTo(
            "5 null null null null null null null null null null null 9999-12-31 null null null null null");
  }

  @Test
  public void testReadDateLowerLimitFromCompositeTable() {
    Dataset<Row> df = readFromTable("integration_composite_table");
    Row row = df.filter("id = 6").first();

    String results = toStringFromCompositeTable(row);
    assertThat(results)
        .isEqualTo(
            "6 null null null null null null null null null null null 1700-01-01 null null null null null");
  }

  @Test
  public void testReadNumericLimitFromCompositeTable() {
    Dataset<Row> df = readFromTable("integration_composite_table");
    Row row = df.filter("id = 7").first();

    String results = toStringFromCompositeTable(row);
    assertThat(results)
        .isEqualTo(
            "7 null null null null null null null null null null null null 99999999999999999999999999999.999999999 -99999999999999999999999999999.999999999 null null null");
  }

  @Test
  public void testReadNumericNanLimitFromCompositeTable() {
    Dataset<Row> df = readFromTable("integration_composite_table");
    Row row = df.filter("id = 9").first();

    String results = toStringFromCompositeTable(row);
    assertThat(results)
        .isEqualTo(
            "9 null null null null null null null null null null null null null null null null null");
  }

  @Test
  public void testReadNumericOutOfScopeFromCompositeTable() {
    Dataset<Row> df = readFromTable("numeric_table");
    Exception e =
        assertThrows(
            Exception.class,
            () -> {
              Row row = df.filter("id = 1").first();
            });
    assertThat(e.getCause().getClass()).isEqualTo(SpannerConnectorException.class);
    assertThat(e.getMessage())
        .contains("The spannner DB may contain Decimal type that is out of scope");
  }

  @Test
  public void testReadTimestampLimitFromCompositeTable() {
    Dataset<Row> df = readFromTable("integration_composite_table");
    Row row = df.filter("id = 8").first();

    String results = toStringFromCompositeTable(row);
    assertThat(results)
        .isEqualTo(
            "8 null null null null null null null null null null null null null null 0001-01-02 06:52:58.0 9999-12-30 09:00:00.0 null");
  }

  @Test
  public void testReadNullFromArrayTable() {
    Dataset<Row> df = readFromTable("array_table");
    Row row = df.filter("id = 1").first();

    String results = toStringFromArrayTable(row);
    assertThat(results).isEqualTo("1 null null null null null null null null null");
  }

  @Test
  public void testReadNullPartiallyFromArrayTable() {
    Dataset<Row> df = readFromTable("array_table");
    Row row = df.filter("id = 2").first();

    String results = toStringFromArrayTable(row);
    assertThat(results)
        .isEqualTo(
            "2 null charvarray null true null 1024 null 1.0E-8 null beefdead null 1999-01-08 null 123450.000000000 null 2003-04-12 11:05:06.0 null {\"tags\": [\"multi-cuisine\", \"open-seating\"], \"rating\": 4.5}");
  }

  private String toStringFromCompositeTable(Row row) {
    String results = toStringFromCompositeTable(row, 0);

    for (int i = 1; i < row.length(); i++) {
      results = results + " " + toStringFromCompositeTable(row, i);
    }
    return results;
  }

  private String toStringFromCompositeTable(Row row, int index) {
    if (row.isNullAt(index)) {
      return "null";
    }
    switch (index) {
      case 0:
        return String.valueOf(row.getLong(index));
      case 1:
      case 2:
      case 3:
      case 17:
        return String.valueOf(row.getString(index));
      case 4:
      case 5:
        return String.valueOf(row.getBoolean(index));
      case 6:
      case 7:
      case 8:
        return String.valueOf(row.getLong(index));
      case 9:
      case 10:
        return String.valueOf(row.getDouble(index));
      case 11:
        return toStringForBytes(row.<byte[]>getAs(index));
      case 12:
        return row.getDate(index).toString();
      case 13:
      case 14:
        return row.getDecimal(index).toString();
      case 15:
      case 16:
        return row.getTimestamp(index).toString();
    }
    return "";
  }

  private String toStringFromArrayTable(Row row) {
    String results = toStringFromCompositeTable(row, 0);

    for (int i = 1; i < row.length(); i++) {
      results = results + " " + toStringFromArrayTable(row, i);
    }
    return results;
  }

  private String toStringFromArrayTable(Row row, int index) {
    if (row.isNullAt(index)) {
      return "null";
    }
    switch (index) {
      case 0:
        return String.valueOf(row.getLong(index));
      case 1:
      case 2:
      case 3:
      case 4:
      case 6:
      case 7:
      case 8:
      case 9:
        return row.getList(index).stream()
            .map(obj -> String.valueOf(obj))
            .collect(Collectors.joining(" "));
      case 5:
        List<byte[]> byteList = row.getList(index);
        return byteList.stream()
            .map(bytes -> toStringForBytes(bytes))
            .collect(Collectors.joining(" "));
    }
    return "";
  }

  private String toStringForBytes(byte[] bytes) {
    return bytes == null ? "null" : new String(bytes, StandardCharsets.UTF_8);
  }

  private String toString(Object object) {
    if (object == null) {
      return "null";
    }
    return String.valueOf(object);
  }

  private String concat(String... strs) {
    return String.join(" ", strs);
  }
}
