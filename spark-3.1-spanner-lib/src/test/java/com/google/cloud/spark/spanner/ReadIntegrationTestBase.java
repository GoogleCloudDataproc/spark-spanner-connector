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
import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.Collator;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import scala.collection.JavaConverters;
import scala.collection.Seq;

public class ReadIntegrationTestBase extends SparkSpannerIntegrationTestBase {

  public Dataset<Row> readFromTable(String table) {
    Map<String, String> props = this.connectionProperties();
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
    Dataset<Row> df = readFromTable("compositeTable");
    assertThat(df.count()).isEqualTo(2);
  }

  @Test
  public void testReadFromCompositeTable() {
    Dataset<Row> df = readFromTable("compositeTable");
    long totalRows = df.count();
    assertThat(totalRows).isEqualTo(2);

    // 1. Validate ids.
    List<String> gotIds = df.select("id").as(Encoders.STRING()).collectAsList();
    List<String> expectIds = Arrays.asList("id1", "id2");
    assertThat(gotIds).containsExactlyElementsIn(expectIds);

    // 2. Validate A field ARRAY<INT64> values.
    List<Row> allAs = df.select("A").collectAsList();
    assertEquals(allAs.size(), 2);
    Row rowA1 = allAs.get(0);
    assertEquals(rowA1.size(), 1);
    Row rowA2 = allAs.get(1);
    assertEquals(rowA2.size(), 1);
    List<Long> gotAs = new ArrayList();
    gotAs.addAll(rowA1.getList(0));
    gotAs.addAll(rowA2.getList(0));
    Collections.sort(gotAs);

    List<Long> wantAFlattened =
        Arrays.stream(new long[] {10, 100, 991, 567282, 20, 200, 2991, 888885})
            .boxed()
            .collect(Collectors.toList());
    Collections.sort(wantAFlattened);
    assertThat(gotAs).containsExactlyElementsIn(wantAFlattened);

    // 3. Validate B field ARRAY<STRING> values.
    List<Row> allBs = df.select("B").collectAsList();
    assertEquals(allBs.size(), 2);
    Row rowB1 = allBs.get(0);
    assertEquals(rowB1.size(), 1);
    Row rowB2 = allBs.get(1);
    assertEquals(rowB2.size(), 1);
    List<String> gotBs = new ArrayList();
    gotBs.addAll(rowB1.getList(0));
    gotBs.addAll(rowB2.getList(0));
    Collections.sort(gotBs);

    List<String> wantBFlattened = Arrays.asList("a", "b", "c", "A", "B", "C");
    Collections.sort(wantBFlattened);
    assertThat(gotBs).containsExactlyElementsIn(wantBFlattened);

    // 4. Validate C field string values.
    List<String> gotCs = df.select("C").as(Encoders.STRING()).collectAsList();
    List<String> expectCs = Arrays.asList("foobar", "this one");
    assertThat(gotCs).containsExactlyElementsIn(expectCs);

    // 5. Validate D field numeric values.
    List<BigDecimal> gotDs = df.select("D").as(Encoders.DECIMAL()).collectAsList();
    List<BigDecimal> expectDs =
        Arrays.asList(asSparkBigDecimal("2934"), asSparkBigDecimal("93411"));
    Collections.sort(gotDs);
    Collections.sort(expectDs);
    assertThat(gotDs).containsExactlyElementsIn(expectDs);

    // 6. Validate E field date values.
    List<Date> gotEs = df.select("E").as(Encoders.DATE()).collectAsList();
    List<Date> expectEs = Arrays.asList(Date.valueOf("2023-01-01"), Date.valueOf("2023-09-23"));
    assertThat(gotEs).containsExactlyElementsIn(expectEs);

    // 7. Validate F field timestamp values.
    List<Timestamp> gotFs = df.select("F").as(Encoders.TIMESTAMP()).collectAsList();
    List<Timestamp> expectFs =
        Arrays.asList(
            asSparkTimestamp("2023-08-26T12:22:05Z"), asSparkTimestamp("2023-09-22T12:22:05Z"));
    assertThat(gotFs).containsExactlyElementsIn(expectFs);

    // 8. Validate G field boolean values.
    List<Boolean> gotGs = df.select("G").as(Encoders.BOOLEAN()).collectAsList();
    List<Boolean> expectGs = Arrays.asList(true, false);
    assertThat(gotGs).containsExactlyElementsIn(expectGs);

    // 9. Validate H field ARRAY<DATE> values.
    List<Row> allHs = df.select("H").collectAsList();
    assertEquals(allHs.size(), 2);
    Row row1H = allHs.get(0);
    assertEquals(row1H.size(), 1);
    Row row2H = allHs.get(1);
    assertEquals(row2H.size(), 1);
    List<Date> gotHs = new ArrayList();
    gotHs.addAll(row1H.getList(0));
    gotHs.addAll(row2H.getList(0));
    Collections.sort(gotHs);

    List<String> expectedHs = Arrays.asList("2023-01-02", "2023-12-31", "2023-09-02", "2023-12-31");

    // It is complicated to compare java.sql.Date values
    // due to timezoning losses after conversion to java.sql.Date
    // hence we shall use String values for the comparisons, because:
    // by timezone pollution:
    //   1. milliseconds: 1672560000000 and 1672617600000
    //        are both equal to:
    //   2023-01-01
    //
    //   2. milliseconds: 1693551600000 and 1693612800000
    //        are both equal to:
    //   2023-09-01
    List<String> gotHSStr = new ArrayList();
    gotHs.forEach(ts -> gotHSStr.add(ts.toString()));
    Collections.sort(gotHs);
    assertThat(gotHSStr).containsExactlyElementsIn(expectedHs);

    // 10. Validate I field ARRAY<TIMESTAMP> values.
    List<Row> allIs = df.select("I").collectAsList();
    assertEquals(allIs.size(), 2);
    Row row1 = allIs.get(0);
    assertEquals(row1.size(), 1);
    Row row2 = allIs.get(1);
    assertEquals(row2.size(), 1);
    List<Timestamp> gotIs = new ArrayList();
    gotIs.addAll(row1.getList(0));
    gotIs.addAll(row2.getList(0));
    Collections.sort(gotIs);

    List<Timestamp> wantIs = new ArrayList();
    Arrays.asList(
            ZonedDateTime.parse("2023-08-26T12:11:10Z"),
            ZonedDateTime.parse("2023-08-27T12:11:09Z"),
            ZonedDateTime.parse("2023-09-22T12:11:10Z"),
            ZonedDateTime.parse("2023-09-23T12:11:09Z"))
        .forEach(zd -> wantIs.add(new Timestamp(zd.toInstant().toEpochMilli())));
    Collections.sort(wantIs);
    assertThat(gotIs).containsExactlyElementsIn(wantIs);

    List<byte[]> gotJs = df.select("J").as(Encoders.BINARY()).collectAsList();
    List<byte[]> expectJs = Arrays.asList(stringToBytes("deadbeef"), stringToBytes("beefdead"));
    List<String> actualStringJs =
        gotJs.stream().map(b -> bytesToString(b)).collect(Collectors.toList());
    List<String> expectedStringJs =
        expectJs.stream().map(b -> bytesToString(b)).collect(Collectors.toList());
    assertThat(actualStringJs).containsExactlyElementsIn(expectedStringJs);
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

    // ARRAY<INT64>: NULL.
    List<Row> allAs = df.select("A").collectAsList();
    assertEquals(allAs.size(), 4);
    List<Long> gotAs = new ArrayList();

    int nullArrayCounts = 0;
    for (int i = 0; i < allAs.size(); i++) {
      Row row = allAs.get(i);

      for (int j = 0; j < row.size(); j++) {
        Object obj = row.get(j);
        if (obj == null) {
          nullArrayCounts++;
          continue;
        }

        List<Long> ai = row.getList(j);
        gotAs.addAll(ai);
      }
    }
    assertEquals(1, nullArrayCounts);

    List<Object> wantAs = Arrays.asList(1L, 2L, null, 2L, 3L, null, null, 4L, 57L, 10L);
    List<Object> gotAsObj = new ArrayList();
    gotAs.forEach(v -> gotAsObj.add(v));

    customComparator cmp = new customComparator();
    Collections.sort(wantAs, cmp);
    Collections.sort(gotAsObj, cmp);
    assertThat(gotAsObj).isEqualTo(wantAs);

    // ARRAY<STRING>: NULL.
    List<Row> allBs = df.select("B").collectAsList();
    assertEquals(allBs.size(), 4);
    List<String> gotBs = new ArrayList();

    nullArrayCounts = 0;
    for (int i = 0; i < allBs.size(); i++) {
      Row row = allBs.get(i);

      for (int j = 0; j < row.size(); j++) {
        Object obj = row.get(j);
        if (obj == null) {
          nullArrayCounts++;
          continue;
        }

        List<String> bj = row.getList(j);
        gotBs.addAll(bj);
      }
    }
    assertEquals(2, nullArrayCounts);

    List<Object> wantBs =
        Arrays.asList(((String) null), ((String) null), "a", "b", "FF", "💡🚨", "b", "fg");
    List<Object> gotBsObj = new ArrayList();
    gotBs.forEach(v -> gotBsObj.add(v));
    Collections.sort(wantBs, cmp);
    Collections.sort(gotBsObj, cmp);
    assertThat(gotBsObj).isEqualTo(wantBs);

    // STRING: NULL.
    List<String> gotCs = df.select("C").as(Encoders.STRING()).collectAsList();
    List<String> wantCs = Arrays.asList(((String) null), ((String) null), "😎🚨", "🚨");

    Collections.sort(wantCs, cmp);
    Collections.sort(gotCs, cmp);
    assertThat(gotCs).isEqualTo(wantCs);

    // NUMERIC: NULL.
    List<BigDecimal> gotDs = df.select("D").as(Encoders.DECIMAL()).collectAsList();
    List<BigDecimal> wantDs =
        Arrays.asList(null, null, asSparkBigDecimal("55.7"), asSparkBigDecimal("99.37171"));
    Collections.sort(gotDs, cmp);
    Collections.sort(wantDs, cmp);
    assertThat(gotDs).isEqualTo(wantDs);

    // DATE: NULL.
    List<Date> gotEs = df.select("E").as(Encoders.DATE()).collectAsList();
    List<Date> wantEs = Arrays.asList(null, null, null, Date.valueOf("2023-12-30"));
    Collections.sort(gotEs, cmp);
    Collections.sort(wantEs, cmp);
    assertThat(gotEs).isEqualTo(wantEs);

    // TIMESTAMP: NULL.
    List<Timestamp> gotFs = df.select("F").as(Encoders.TIMESTAMP()).collectAsList();
    List<Timestamp> wantFs =
        Arrays.asList(null, null, null, asSparkTimestamp("2023-09-23T12:11:09Z"));
    Collections.sort(gotFs, cmp);
    Collections.sort(wantFs, cmp);
    assertThat(gotFs).isEqualTo(wantFs);

    // BOOL: NULL.
    List<Boolean> gotGs = df.select("G").as(Encoders.BOOLEAN()).sort().collectAsList();
    List<Boolean> wantGs = Arrays.asList(null, true, false, false);
    Collections.sort(gotGs, cmp);
    Collections.sort(wantGs, cmp);
    assertThat(gotGs).isEqualTo(wantGs);

    // ARRAY<DATE>: NULL.
    List<Row> allHs = df.select("H").collectAsList();
    assertEquals(allHs.size(), 4);
    List<Date> gotHs = new ArrayList();

    nullArrayCounts = 0;
    for (int i = 0; i < allHs.size(); i++) {
      Row row = allHs.get(i);

      for (int j = 0; j < row.size(); j++) {
        Object obj = row.get(j);
        if (obj == null) {
          nullArrayCounts++;
          continue;
        }

        List<Date> dj = row.getList(j);
        gotHs.addAll(dj);
      }
    }
    assertEquals(2, nullArrayCounts);

    List<Object> wantHs =
        Arrays.asList(
            ((Date) null), ((Date) null), Date.valueOf("2022-10-01"), Date.valueOf("2023-09-22"));
    List<Object> gotHsObj = new ArrayList();
    gotHs.forEach(v -> gotHsObj.add(v));
    Collections.sort(wantHs, cmp);
    Collections.sort(gotHsObj, cmp);
    assertThat(gotHsObj).isEqualTo(wantHs);

    // ARRAY<TIMESTAMP>: NULL.
    List<Row> allIs = df.select("I").collectAsList();
    assertEquals(allIs.size(), 4);
    List<Timestamp> gotIs = new ArrayList();

    nullArrayCounts = 0;
    for (int i = 0; i < allIs.size(); i++) {
      Row row = allIs.get(i);

      for (int j = 0; j < row.size(); j++) {
        Object obj = row.get(j);
        if (obj == null) {
          nullArrayCounts++;
          continue;
        }

        List<Timestamp> tj = row.getList(j);
        gotIs.addAll(tj);
      }
    }
    assertEquals(3, nullArrayCounts);

    List<Object> wantIs =
        Arrays.asList(((Timestamp) null), asSparkTimestamp("2023-09-23T12:11:09Z"));
    List<Object> gotIsObj = new ArrayList();
    gotIs.forEach(v -> gotIsObj.add(v));
    Collections.sort(wantIs, cmp);
    Collections.sort(gotIsObj, cmp);
    assertThat(gotIsObj).isEqualTo(wantIs);

    // ARRAY<BOOL>: with some NULLs.
    List<Row> allJs = df.select("J").collectAsList();
    assertEquals(allJs.size(), 4);
    List<Boolean> gotJs = new ArrayList();

    nullArrayCounts = 0;
    for (int i = 0; i < allJs.size(); i++) {
      Row row = allJs.get(i);

      for (int j = 0; j < row.size(); j++) {
        Object obj = row.get(j);
        if (obj == null) {
          nullArrayCounts++;
          continue;
        }

        List<Boolean> aj = row.getList(j);
        gotJs.addAll(aj);
      }
    }
    assertEquals(1, nullArrayCounts);

    List<Object> gotJsObj = new ArrayList();
    gotJs.forEach(v -> gotJsObj.add(v));

    Boolean nullBool = ((Boolean) null);
    List<Object> wantJs =
        Arrays.asList(nullBool, nullBool, nullBool, true, true, true, true, false);
    Collections.sort(wantJs, cmp);
    Collections.sort(gotJsObj, cmp);
    assertThat(gotJsObj).isEqualTo(wantJs);

    // ARRAY<FLOAT64>: with some NULLs.
    List<Row> allKs = df.select("K").collectAsList();
    assertEquals(allKs.size(), 4);
    List<Double> gotKs = new ArrayList();

    for (int i = 0; i < allKs.size(); i++) {
      Row row = allKs.get(i);

      for (int j = 0; j < row.size(); j++) {
        Object obj = row.get(j);
        if (obj == null) {
          nullArrayCounts++;
          continue;
        }

        List<Double> kj = row.getList(j);
        gotKs.addAll(kj);
      }
    }
    assertEquals(1, nullArrayCounts);

    List<Object> gotKsObj = new ArrayList();
    gotKs.forEach(v -> gotKsObj.add(v));

    Double nullDouble = ((Double) null);
    List<Object> wantKs =
        Arrays.asList(nullDouble, nullDouble, 23.67, 198.1827, -28888.8888, 0.12, 0.71);
    Collections.sort(wantKs, cmp);
    Collections.sort(gotKsObj, cmp);
    assertThat(gotKsObj).isEqualTo(wantKs);

    nullArrayCounts = 0;

    // ARRAY<Bytes>: NULL.
    List<Row> allMs = df.select("M").collectAsList();
    assertEquals(allMs.size(), 4);

    nullArrayCounts = 0;
    for (int i = 0; i < allMs.size(); i++) {
      Row row = allMs.get(i);

      for (int j = 0; j < row.size(); j++) {
        Object obj = row.get(j);
        if (obj == null) {
          nullArrayCounts++;
          continue;
        }
        byte[] expectedBytes = stringToBytes("beefdead");
        assertArrayEquals(row.getList(j), Arrays.asList(null, expectedBytes));
      }
    }
    assertEquals(3, nullArrayCounts);

    // ARRAY<JSON>: NULL.
    List<Row> allNs = df.select("N").collectAsList();
    assertEquals(allNs.size(), 4);

    nullArrayCounts = 0;
    for (int i = 0; i < allNs.size(); i++) {
      Row row = allNs.get(i);

      for (int j = 0; j < row.size(); j++) {
        Object obj = row.get(j);
        if (obj == null) {
          nullArrayCounts++;
          continue;
        }

        assertThat(row.getList(j)).containsExactly(null, "{\"a\":1}");
      }
    }
    assertEquals(3, nullArrayCounts);

    // ARRAY<NUMERIC>: with NULLs.
    List<Row> allOs = df.select("O").collectAsList();
    assertEquals(allOs.size(), 4);
    List<BigDecimal> gotOs = new ArrayList();

    nullArrayCounts = 0;
    for (int i = 0; i < allOs.size(); i++) {
      Row row = allOs.get(i);

      for (int j = 0; j < row.size(); j++) {
        Object obj = row.get(j);
        if (obj == null) {
          nullArrayCounts++;
          continue;
        }

        List<BigDecimal> ai = row.getList(j);
        gotOs.addAll(ai);
      }
    }
    assertEquals(1, nullArrayCounts);

    List<Object> wantOs =
        Arrays.asList(
            null,
            null,
            null,
            asSparkBigDecimal("-55.7"),
            asSparkBigDecimal("-99.37171"),
            asSparkBigDecimal("12"),
            asSparkBigDecimal("9.3"));
    List<Object> gotOsAsObj = new ArrayList();
    gotOs.forEach(v -> gotOsAsObj.add(v));
    Collections.sort(wantOs, cmp);
    Collections.sort(gotOsAsObj, cmp);
    assertThat(gotOsAsObj).isEqualTo(wantOs);
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
    if (true) {
      // TIMESTAMP extreme limit tests require lots of mucking around
      // and have held up code releases, so this code is commented out
      // until we have more time to focus on them: @odeke-em
      // A follow-up PR will be sent later.
      return;
    }

    Dataset<Row> df = readFromTable("valueLimitsTable");
    assertThat(df.count()).isEqualTo(4);

    // 1. Test the limits of TIMESTAMP.
    List<Timestamp> gotEs = df.select("E").as(Encoders.TIMESTAMP()).collectAsList();
    Collections.sort(gotEs);

    List<Timestamp> expectEs =
        Arrays.asList(
            zdtToTs("0001-01-01T00:00:01Z"),
            zdtToTs("2023-09-28T21:59:59Z"),
            zdtToTs("2222-02-22T22:22:22Z"),
            zdtToTs("9999-12-30T23:59:59.00Z"));
    Collections.sort(expectEs);
    assertThat(gotEs).containsExactlyElementsIn(expectEs);
  }

  Timestamp zdtToTs(String s) {
    return new Timestamp(ZonedDateTime.parse(s).toInstant().toEpochMilli());
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
}
