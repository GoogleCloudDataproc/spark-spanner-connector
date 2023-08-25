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

package com.google.cloud.spark;

import static org.junit.Assert.assertEquals;

import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spark.spanner.SpannerScanBuilder;
import java.util.Arrays;
import java.util.Map;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SpannerScanBuilderTest {

  Spanner spanner = SpannerUtilsTest.createSpanner();
  BatchClient batchClient =
      SpannerUtilsTest.createBatchClient(spanner, SpannerUtilsTest.connectionProperties());

  @Before
  public void setUp() throws Exception {
    // 1. Insert some 10 rows.
  }

  @After
  public void teardown() throws Exception {
    // 1. Delete all the contents.
    spanner.close();
  }

  @Test
  public void testReadSchema() throws Exception {
    Map<String, String> opts = SpannerUtilsTest.connectionProperties();
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
  public void testPlanInputPartitions() throws Exception {
    Map<String, String> opts = SpannerUtilsTest.connectionProperties();
    CaseInsensitiveStringMap copts = new CaseInsensitiveStringMap(opts);
    SpannerScanBuilder ssb = new SpannerScanBuilder(copts);
    InputPartition[] partitions = ssb.planInputPartitions();
    for (InputPartition part : partitions) {
      System.out.println("Partition: " + part);
    }
  }
}
