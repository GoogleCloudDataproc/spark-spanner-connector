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

import java.util.Arrays;
import java.util.Map;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SpannerTableTest extends SpannerTestBase {

  @Test
  public void querySchemaShouldSuccessInSpannerTable() {
    Map<String, String> props = this.connectionProperties();
    SpannerTable spannerTable = new SpannerTable(props);
    StructType actualSchema = spannerTable.schema();
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
  public void queryPgSchemaShouldSuccessInSpannerTable() {
    Map<String, String> props = this.connectionProperties(/* usePostgreSql= */ true);
    SpannerTable spannerTable = new SpannerTable(props);
    StructType actualSchema = spannerTable.schema();
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
}
