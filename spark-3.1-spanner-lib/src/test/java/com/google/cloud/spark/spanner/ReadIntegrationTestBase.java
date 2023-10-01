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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

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

    // 2. Validate C field string values.
    List<String> gotCs = df.select("C").as(Encoders.STRING()).collectAsList();
    List<String> expectCs = Arrays.asList("foobar", "this one");
    assertThat(gotCs).containsExactlyElementsIn(expectCs);

    // 3. Validate G field boolean values.
    List<Boolean> gotGs = df.select("G").as(Encoders.BOOLEAN()).collectAsList();
    List<Boolean> expectGs = Arrays.asList(true, false);
    assertThat(gotGs).containsExactlyElementsIn(expectGs);

    // 4. Validate E field date values.
    List<Date> gotEs = df.select("E").as(Encoders.DATE()).collectAsList();
    List<Date> expectEs = Arrays.asList(Date.valueOf("2022-12-31"), Date.valueOf("2023-09-22"));
    assertThat(gotEs).containsExactlyElementsIn(expectEs);

    // 5. Validate F field timestamp values.
    List<Timestamp> gotFs = df.select("F").as(Encoders.TIMESTAMP()).collectAsList();
    List<Timestamp> expectFs =
        Arrays.asList(
            new Timestamp(ZonedDateTime.parse("2023-08-26T12:22:05Z").toInstant().toEpochMilli()),
            new Timestamp(ZonedDateTime.parse("2023-09-22T12:22:05Z").toInstant().toEpochMilli()));
    assertThat(gotFs).containsExactlyElementsIn(expectFs);

    // 6. Validate D field numeric values.
    List<BigDecimal> gotDs = df.select("D").as(Encoders.DECIMAL()).collectAsList();
    List<BigDecimal> expectDs =
        Arrays.asList(asSparkBigDecimal("2934000000000"), asSparkBigDecimal("93411000000000"));
    Collections.sort(gotDs);
    Collections.sort(expectDs);
    assertThat(gotDs).containsExactlyElementsIn(expectDs);
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
                        "I", DataTypes.createArrayType(DataTypes.TimestampType, true), true, null))
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

  BigDecimal asSparkBigDecimal(String v) {
    return new BigDecimal(new BigInteger(v), 9, new MathContext(38));
  }
}
