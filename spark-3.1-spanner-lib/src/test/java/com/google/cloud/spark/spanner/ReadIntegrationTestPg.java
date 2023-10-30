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
    assertThat(df.count()).isEqualTo(9);
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
