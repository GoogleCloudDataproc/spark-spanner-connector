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

import static org.junit.Assert.assertEquals;

import com.google.cloud.spark.spanner.*;
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
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnit4.class)
public abstract class SpannerScanBuilderIntegrationTestBase extends SpannerTestBase {
  private static final Logger logger =
      LoggerFactory.getLogger(SpannerScanBuilderIntegrationTestBase.class);

  private static SpannerTable getSpannerTable(boolean usePostgreSql) {
    Map<String, String> connectionProperties = connectionProperties(usePostgreSql);
    return new SpannerTable(connectionProperties);
  }

  protected static SpannerTable getSpannerTable(String tableName, boolean usePostgreSql) {
    Map<String, String> connectionProperties = connectionProperties(usePostgreSql);
    connectionProperties.put("table", tableName);
    return new SpannerTable(connectionProperties);
  }

  @Test
  public void readSchemaShouldWorkInSpannerScanBuilder() throws Exception {
    Scan scan = new SpannerScanBuilder(getSpannerTable(false)).build();
    StructType actualSchema = scan.readSchema();
    StructType expectSchema = SpannerTestBase.getATableSchema();

    // Object.equals fails for StructType with fields so we'll
    // firstly compare lengths, then fieldNames then the simpleString.
    assertEquals(expectSchema.length(), actualSchema.length());
    assertEquals(expectSchema.fieldNames(), actualSchema.fieldNames());
    assertEquals(expectSchema.simpleString(), actualSchema.simpleString());
  }

  @Test
  public void readSchemaShouldWorkInSpannerScanBuilderForPg() throws Exception {
    if (emulatorHost != null && !emulatorHost.isEmpty()) {
      // Spanner emulator doesn't support the PostgreSql dialect interface.
      // If the emulator is set. We return immediately here.
      logger.info(
          "readSchemaShouldWorkInSpannerScanBuilderForPg is skipped since pg is not supported in Spanner emulator");
      return;
    }
    Scan scan = new SpannerScanBuilder(getSpannerTable(true)).build();
    StructType actualSchema = scan.readSchema();
    StructType expectSchema = SpannerTestBase.getCompositeTableSchema();

    // Object.equals fails for StructType with fields so we'll
    // firstly compare lengths, then fieldNames then the simpleString.
    assertEquals(expectSchema.length(), actualSchema.length());
    assertEquals(expectSchema.fieldNames(), actualSchema.fieldNames());
    assertEquals(expectSchema.simpleString(), actualSchema.simpleString());
  }

  public void planInputPartitionsShouldSuccessInSpannerScanBuilderBase(
      String tableName,
      boolean usePostgreSql,
      List<InternalRow> expectRows,
      StructType expectSchema)
      throws Exception {
    SpannerScanBuilder spannerScanBuilder =
        new SpannerScanBuilder(getSpannerTable(tableName, usePostgreSql));
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

    Collections.sort(expectRows, cmp);

    assertEquals(expectRows.size(), gotRows.size());

    assertEquals(normalizeRows(expectRows, expectSchema), normalizeRows(gotRows, ss.readSchema()));
  }

  @Test
  public void planInputPartitionsShouldSuccessInSpannerScanBuilder() throws Exception {
    List<InternalRow> expectRows =
        new ArrayList<>(
            Arrays.asList(
                makeATableInternalRow(
                    1,
                    "2",
                    null,
                    ZonedDateTime.parse("2023-08-22T12:22:00Z"),
                    1000.282111401,
                    true,
                    -0.1,
                    com.google.cloud.Date.parseDate("2023-12-31"),
                    null,
                    null,
                    -0.1),
                makeATableInternalRow(
                    10,
                    "20",
                    null,
                    ZonedDateTime.parse("2023-08-22T12:23:00Z"),
                    10000.282111603,
                    false,
                    0.2,
                    com.google.cloud.Date.parseDate("2024-01-01"),
                    null,
                    null,
                    null),
                makeATableInternalRow(
                    30,
                    "30",
                    null,
                    ZonedDateTime.parse("2023-08-22T12:24:00Z"),
                    30000.282111805,
                    false,
                    0.1,
                    com.google.cloud.Date.parseDate("2025-12-31"),
                    null,
                    null,
                    -0.1),
                makeATableInternalRow(
                    40,
                    "40",
                    new byte[] {'x', 'y', 'z'},
                    ZonedDateTime.parse("2025-01-01T12:34:56Z"),
                    123.456789123,
                    true,
                    3.14,
                    com.google.cloud.Date.parseDate("2026-01-01"),
                    new String[] {"a", "b", "c"},
                    "{\"name\":\"john\"}",
                    null),
                makeATableInternalRow(
                    50, null, null, null, null, null, null, null, null, null, null)));

    planInputPartitionsShouldSuccessInSpannerScanBuilderBase(
        "ATable", false, expectRows, SpannerTestBase.getATableSchema());
  }

  @Test
  public void planInputPartitionsShouldSucceedInSpannerScanBuilderPg() throws Exception {
    if (emulatorHost != null && !emulatorHost.isEmpty()) {
      // Spanner emulator doesn't support the PostgreSql dialect interface.
      // If the emulator is set. We return immediately here.
      logger.info(
          "planInputPartitionsShouldSucceedInSpannerScanBuilderPg is skipped since pg is not supported in Spanner emulator");
      return;
    }

    List<InternalRow> expectRows =
        new ArrayList<>(
            Arrays.asList(
                makeCompositeTableRowPg(
                    1, null, null, null, null, null, null, null, null, null, null, null, null, null,
                    null, null, null, null),
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
                    new byte[] {'b', 'e', 'e', 'f', 'd', 'e', 'a', 'd'},
                    "1999-01-08T04:24:00Z",
                    new java.math.BigDecimal("123456"),
                    new java.math.BigDecimal("9e23"),
                    "2003-04-12T11:05:06Z",
                    "2003-04-12T12:05:06Z",
                    "{\"tags\": [\"multi-cuisine\", \"open-seating\"], \"rating\": 4.5}")));

    planInputPartitionsShouldSuccessInSpannerScanBuilderBase(
        "composite_table", true, expectRows, SpannerTestBase.getCompositeTableSchema());
  }

  private static List<List<Object>> normalizeRows(List<InternalRow> rows, StructType schema) {

    List<List<Object>> normalized = new ArrayList<>();

    for (InternalRow row : rows) {
      List<Object> values = new ArrayList<>();

      for (int i = 0; i < schema.fields().length; i++) {
        StructField field = schema.fields()[i];
        DataType type = field.dataType();

        if (row.isNullAt(i)) {
          values.add(null);
        } else {
          values.add(normalizeValue(row.get(i, type), type));
        }
      }

      normalized.add(values);
    }

    return normalized;
  }

  private static Object normalizeValue(Object value, DataType type) {

    if (value == null) {
      return null;
    }

    if (type.sameType(DataTypes.BinaryType)) {
      return Arrays.toString((byte[]) value);
    }

    if (type instanceof DecimalType) {
      if (value instanceof Decimal) {
        return ((Decimal) value).toJavaBigDecimal();
      }
      return value;
    }

    if (type instanceof ArrayType) {
      return normalizeArray(value, (ArrayType) type);
    }

    if (value instanceof UTF8String) {
      return value.toString();
    }

    return value;
  }

  private static List<Object> normalizeArray(Object value, ArrayType arrayType) {

    List<Object> result = new ArrayList<>();
    DataType elementType = arrayType.elementType();

    if (value instanceof ArrayData) {
      ArrayData array = (ArrayData) value;

      for (int i = 0; i < array.numElements(); i++) {
        result.add(
            array.isNullAt(i) ? null : normalizeValue(array.get(i, elementType), elementType));
      }
    } else if (value instanceof List<?>) {
      for (Object element : (List<?>) value) {
        result.add(normalizeValue(element, elementType));
      }
    } else {
      throw new IllegalArgumentException("Unsupported array representation: " + value.getClass());
    }

    return result;
  }
}
