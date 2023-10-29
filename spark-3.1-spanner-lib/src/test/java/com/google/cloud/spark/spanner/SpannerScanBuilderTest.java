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

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SpannerScanBuilderTest extends SpannerTestBase {

  @Test
  public void readSchemaShouldWorkInSpannerScanBuilder() throws Exception {
    Map<String, String> opts = this.connectionProperties();
    CaseInsensitiveStringMap copts = new CaseInsensitiveStringMap(opts);
    Scan scan = new SpannerScanBuilder(copts).build();
    StructType actualSchema = scan.readSchema();
    StructType expectSchema =
        new StructType(
            Arrays.asList(
                    new StructField("A", DataTypes.LongType, false, null),
                    new StructField("B", DataTypes.StringType, true, null),
                    new StructField("C", DataTypes.BinaryType, true, null),
                    new StructField("D", DataTypes.TimestampType, true, null),
                    new StructField("E", DataTypes.createDecimalType(38, 9), true, null),
                    new StructField(
                        "F", DataTypes.createArrayType(DataTypes.StringType, true), true, null))
                .toArray(new StructField[0]));

    // Object.equals fails for StructType with fields so we'll
    // firstly compare lengths, then fieldNames then the simpleString.
    assertEquals(expectSchema.length(), actualSchema.length());
    assertEquals(expectSchema.fieldNames(), actualSchema.fieldNames());
    assertEquals(expectSchema.simpleString(), actualSchema.simpleString());
  }

  @Test
  public void readSchemaShouldWorkInSpannerScanBuilderForPg() throws Exception {
    if (SpannerScanBuilderTest.emulatorHost != null
        && !SpannerScanBuilderTest.emulatorHost.isEmpty()) {
      // Spanner emulator doesn't support the PostgreSql dialect interface.
      // If the emulator is set. We return immediately here.
      // TODO: Use logger instead of System out once logger configuration is set.
      System.out.println(
          "readSchemaShouldWorkInSpannerScanBuilderForPg is skipped since pg is not supported in Spanner emulator");
      return;
    }
    Map<String, String> opts = this.connectionProperties(/* usePostgreSql= */ true);
    CaseInsensitiveStringMap copts = new CaseInsensitiveStringMap(opts);
    Scan scan = new SpannerScanBuilder(copts).build();
    StructType actualSchema = scan.readSchema();
    StructType expectSchema =
        new StructType(
            Arrays.asList(
                    new StructField("id", DataTypes.LongType, false, null),
                    new StructField("charvcol", DataTypes.StringType, true, null),
                    new StructField("textcol", DataTypes.StringType, true, null),
                    new StructField("varcharcol", DataTypes.StringType, true, null),
                    new StructField("boolcol", DataTypes.BooleanType, true, null),
                    new StructField("booleancol", DataTypes.BooleanType, true, null),
                    new StructField("bigintcol", DataTypes.LongType, true, null),
                    new StructField("int8col", DataTypes.LongType, true, null),
                    new StructField("intcol", DataTypes.LongType, true, null),
                    new StructField("doublecol", DataTypes.DoubleType, true, null),
                    new StructField("floatcol", DataTypes.DoubleType, true, null),
                    new StructField("bytecol", DataTypes.BinaryType, true, null),
                    new StructField("datecol", DataTypes.DateType, true, null),
                    new StructField("numericcol", DataTypes.createDecimalType(38, 9), true, null),
                    new StructField("decimalcol", DataTypes.createDecimalType(38, 9), true, null),
                    new StructField("timewithzonecol", DataTypes.TimestampType, true, null),
                    new StructField("timestampcol", DataTypes.TimestampType, true, null))
                .toArray(new StructField[0]));

    // Object.equals fails for StructType with fields so we'll
    // firstly compare lengths, then fieldNames then the simpleString.
    assertEquals(expectSchema.length(), actualSchema.length());
    assertEquals(expectSchema.fieldNames(), actualSchema.fieldNames());
    assertEquals(expectSchema.simpleString(), actualSchema.simpleString());
  }

  @Test
  public void planInputPartitionsShouldSuccessInSpannerScanBuilder() throws Exception {
    Map<String, String> opts = this.connectionProperties();
    opts.put("table", "ATable");
    CaseInsensitiveStringMap optionMap = new CaseInsensitiveStringMap(opts);
    SpannerScanBuilder spannerScanBuilder = new SpannerScanBuilder(optionMap);
    SpannerScanner ss = ((SpannerScanner) spannerScanBuilder.build());
    InputPartition[] partitions = ss.planInputPartitions();
    PartitionReaderFactory prf = ss.createReaderFactory();
    CopyOnWriteArrayList<InternalRow> al = new CopyOnWriteArrayList<>();
    for (InputPartition partition : partitions) {
      PartitionReader<InternalRow> ir = prf.createReader(partition);
      try {
        while (ir.next()) {
          al.add(ir.get());
        }
        SpannerPartitionReader sr = ((SpannerPartitionReader) ir);
        sr.close();
      } catch (IOException e) {
      }
    }

    if (prf instanceof SpannerInputPartitionReaderContext) {
      try {
        SpannerInputPartitionReaderContext spc = ((SpannerInputPartitionReaderContext) prf);
        spc.close();
      } catch (IOException e) {
      }
    }

    List<InternalRow> gotRows = new ArrayList<>();
    al.forEach(gotRows::add);

    Comparator<InternalRow> cmp = new InternalRowComparator();
    Collections.sort(gotRows, cmp);

    List<InternalRow> expectRows =
        new ArrayList<>(
            Arrays.asList(
                makeATableInternalRow(
                    1,
                    "2",
                    null,
                    ZonedDateTime.parse("2023-08-22T12:22:00Z"),
                    1000.282111401,
                    null),
                makeATableInternalRow(
                    10,
                    "20",
                    null,
                    ZonedDateTime.parse("2023-08-22T12:23:00Z"),
                    10000.282111603,
                    null),
                makeATableInternalRow(
                    30,
                    "30",
                    null,
                    ZonedDateTime.parse("2023-08-22T12:24:00Z"),
                    30000.282111805,
                    null)));
    Collections.sort(expectRows, cmp);

    assertEquals(expectRows.size(), gotRows.size());
    assertEquals(expectRows, gotRows);
  }

  @Test
  public void planInputPartitionsShouldSucceedInSpannerScanBuilderPg() throws Exception {
    if (SpannerTableTest.emulatorHost != null && !SpannerTableTest.emulatorHost.isEmpty()) {
      // Spanner emulator doesn't support the PostgreSql dialect interface.
      // If the emulator is set. We return immediately here.
      // TODO: Use logger instead of System out once logger configuration is set.
      System.out.println(
          "planInputPartitionsShouldSucceedInSpannerScanBuilderPg is skipped since pg is not supported in Spanner emulator");
      return;
    }
    Map<String, String> opts = this.connectionProperties(true);
    opts.put("table", "composite_table");
    CaseInsensitiveStringMap optionMap = new CaseInsensitiveStringMap(opts);
    SpannerScanBuilder spannerScanBuilder = new SpannerScanBuilder(optionMap);
    SpannerScanner ss = ((SpannerScanner) spannerScanBuilder.build());
    InputPartition[] partitions = ss.planInputPartitions();
    PartitionReaderFactory prf = ss.createReaderFactory();
    CopyOnWriteArrayList<InternalRow> al = new CopyOnWriteArrayList<>();
    for (InputPartition partition : partitions) {
      PartitionReader<InternalRow> ir = prf.createReader(partition);
      try {
        while (ir.next()) {
          al.add(ir.get());
        }
        SpannerPartitionReader sr = ((SpannerPartitionReader) ir);
        sr.close();
      } catch (IOException e) {
      }
    }

    if (prf instanceof SpannerInputPartitionReaderContext) {
      try {
        SpannerInputPartitionReaderContext spc = ((SpannerInputPartitionReaderContext) prf);
        spc.close();
      } catch (IOException e) {
      }
    }

    List<InternalRow> gotRows = new ArrayList<>();
    al.forEach(gotRows::add);

    Comparator<InternalRow> cmp = new InternalRowComparator();
    Collections.sort(gotRows, cmp);

    List<InternalRow> expectRows =
        new ArrayList<>(
            Arrays.asList(
                makeCompositeTableRowPg(
                    1, null, null, null, null, null, null, null, null, null, null, null, null, null,
                    null, null, null),
                makeCompositeTableRowPg(
                    2,
                    "charvcol",
                    "textcol",
                    "varcharcol",
                    true,
                    false,
                    1L,
                    -1L,
                    0L,
                    0.00000001,
                    0.00000001,
                    "beefdead",
                    "1999-01-08T04:24:00Z",
                    new java.math.BigDecimal("123456"),
                    new java.math.BigDecimal("9e23"),
                    "2003-04-12T11:05:06Z",
                    "2003-04-12T12:05:06Z")));
    Collections.sort(expectRows, cmp);

    assertEquals(expectRows.size(), gotRows.size());
    assertInternalRowPg(gotRows, expectRows);
  }

  private static void assertInternalRowPg(
      List<InternalRow> actualRows, List<InternalRow> expectedRows) {
    assertEquals(expectedRows.size(), actualRows.size());
    for (int i = 0; i < actualRows.size(); i++) {
      // We cannot use assertEqual for the whole List, since the byte[] will be
      // compared with the object's address.
      GenericInternalRow actualRow = (GenericInternalRow) actualRows.get(i);
      GenericInternalRow expectedRow = (GenericInternalRow) expectedRows.get(i);

      assertThat(actualRow.getLong(0)).isEqualTo(expectedRow.getLong(0));
      assertThat(actualRow.getUTF8String(1)).isEqualTo(expectedRow.getUTF8String(1));
      assertThat(actualRow.getUTF8String(2)).isEqualTo(expectedRow.getUTF8String(2));
      assertThat(actualRow.getUTF8String(3)).isEqualTo(expectedRow.getUTF8String(3));
      assertThat(actualRow.getBoolean(4)).isEqualTo(expectedRow.getBoolean(4));
      assertThat(actualRow.getBoolean(5)).isEqualTo(expectedRow.getBoolean(5));
      assertThat(actualRow.getLong(6)).isEqualTo(expectedRow.getLong(6));
      assertThat(actualRow.getLong(7)).isEqualTo(expectedRow.getLong(7));
      assertThat(actualRow.getLong(8)).isEqualTo(expectedRow.getLong(8));
      assertThat(actualRow.getDouble(9)).isEqualTo(expectedRow.getDouble(9));
      assertThat(actualRow.getDouble(10)).isEqualTo(expectedRow.getDouble(10));
      assertThat(bytesToString(actualRow.getBinary(11)))
          .isEqualTo(
              bytesToString(
                  expectedRow.getUTF8String(11) == null
                      ? null
                      : expectedRow.getUTF8String(11).getBytes()));
      assertThat(actualRow.getInt(12)).isEqualTo(expectedRow.getInt(12));
      assertThat(actualRow.getDecimal(13, 38, 9)).isEqualTo(expectedRow.getDecimal(13, 38, 9));
      assertThat(actualRow.getDecimal(14, 38, 9)).isEqualTo(expectedRow.getDecimal(14, 38, 9));
      assertThat(actualRow.getLong(15)).isEqualTo(expectedRow.getLong(15));
      assertThat(actualRow.getLong(16)).isEqualTo(expectedRow.getLong(16));
    }
  }

  private static String bytesToString(byte[] bytes) {
    return bytes == null ? "" : new String(bytes);
  }
}
