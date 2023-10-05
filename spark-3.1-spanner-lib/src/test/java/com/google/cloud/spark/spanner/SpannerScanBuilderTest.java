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
                    new StructField(
                        "C", DataTypes.createArrayType(DataTypes.ByteType, true), true, null),
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
}
