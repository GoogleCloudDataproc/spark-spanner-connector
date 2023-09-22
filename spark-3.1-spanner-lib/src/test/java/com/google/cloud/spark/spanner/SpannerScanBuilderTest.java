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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
    CaseInsensitiveStringMap optionMap = new CaseInsensitiveStringMap(opts);
    SpannerScanBuilder spannerScanBuilder = new SpannerScanBuilder(optionMap);
    SpannerScanner ss = ((SpannerScanner) spannerScanBuilder.build());
    InputPartition[] partitions = ss.planInputPartitions();
    PartitionReaderFactory prf = ss.createReaderFactory();
    List<InternalRow> rows = new ArrayList<>();
    for (InputPartition partition : partitions) {
      PartitionReader<InternalRow> ir = prf.createReader(partition);
      try {
        while (ir.next()) {
          InternalRow row = ir.get();
          System.out.println("row: " + row.toString());
        }
      } catch (IOException e) {
      }
    }
  }
}
