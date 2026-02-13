// Copyright 2025 Google LLC
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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spark.spanner.TestData;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public abstract class WriteIntegrationTest extends SparkSpannerIntegrationTestBase {

  private final boolean usePostgresSql;
  protected static final String WRITE_ARRAY_TABLE_NAME = "write_array_test_table";

  @Parameters
  public static Collection<Object[]> usePostgresSqlValues() {
    return Arrays.asList(new Object[][] {{false}, {true}});
  }

  public WriteIntegrationTest(boolean usePostgresSql) {
    super();
    this.usePostgresSql = usePostgresSql;
  }

  @Test
  public void testWriteWithNulls() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("long_col", DataTypes.LongType, false),
              DataTypes.createStructField("string_col", DataTypes.StringType, true),
              DataTypes.createStructField("bool_col", DataTypes.BooleanType, true),
              DataTypes.createStructField("double_col", DataTypes.DoubleType, true),
              DataTypes.createStructField("timestamp_col", DataTypes.TimestampType, true),
              DataTypes.createStructField("date_col", DataTypes.DateType, true),
              DataTypes.createStructField("bytes_col", DataTypes.BinaryType, true),
              DataTypes.createStructField("numeric_col", DataTypes.createDecimalType(38, 9), true),
              DataTypes.createStructField(
                  "long_array", DataTypes.createArrayType(DataTypes.LongType), true),
              DataTypes.createStructField(
                  "str_array", DataTypes.createArrayType(DataTypes.StringType), true),
              DataTypes.createStructField(
                  "boolean_array", DataTypes.createArrayType(DataTypes.BooleanType), true),
              DataTypes.createStructField(
                  "double_array", DataTypes.createArrayType(DataTypes.DoubleType), true),
              DataTypes.createStructField(
                  "timestamp_array", DataTypes.createArrayType(DataTypes.TimestampType), true),
              DataTypes.createStructField(
                  "date_array", DataTypes.createArrayType(DataTypes.DateType), true),
              DataTypes.createStructField(
                  "binary_array", DataTypes.createArrayType(DataTypes.BinaryType), true),
              DataTypes.createStructField(
                  "numeric_array",
                  DataTypes.createArrayType(DataTypes.createDecimalType(38, 9)),
                  true),
            });

    List<Row> rows =
        Collections.singletonList(
            RowFactory.create(
                3L, null, null, null, null, null, null, null, null, null, null, null, null, null,
                null, null));

    Dataset<Row> df = spark.createDataFrame(rows, schema);

    Map<String, String> props = connectionProperties(usePostgresSql);
    props.put("table", WRITE_ARRAY_TABLE_NAME);

    df.write().format("cloud-spanner").options(props).mode(SaveMode.Append).save();

    Dataset<Row> writtenDf =
        spark.read().format("cloud-spanner").options(props).load().filter("long_col = 3");

    assertEquals(1, writtenDf.count());
    Row writtenRow = writtenDf.first();

    assertThat(writtenRow.getLong(0)).isEqualTo(3L);
    for (int i = 1; i < schema.length(); i++) {
      assertNull("Column " + schema.fields()[i].name() + " should be null", writtenRow.get(i));
    }
  }

  @Test
  public void testIdempotentWrite() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("long_col", DataTypes.LongType, false),
              DataTypes.createStructField("string_col", DataTypes.StringType, true),
            });

    List<Row> rows = Arrays.asList(RowFactory.create(4L, "four"), RowFactory.create(5L, "five"));

    Dataset<Row> df = spark.createDataFrame(rows, schema);

    Map<String, String> props = connectionProperties(usePostgresSql);
    props.put("table", TestData.WRITE_TABLE_NAME);
    props.put("assumeIdempotentWrites", "true");
    props.put("enablePartialRowUpdates", "true");

    df.write().format("cloud-spanner").options(props).mode(SaveMode.Append).save();

    Dataset<Row> writtenDf =
        spark.read().format("cloud-spanner").options(props).load().filter("long_col IN (4, 5)");

    assertEquals(2, writtenDf.count());
    List<Row> writtenRows = writtenDf.collectAsList();
    List<Long> actual =
        writtenRows.stream()
            .map(row -> row.getLong(0))
            .sorted()
            .collect(java.util.stream.Collectors.toList());

    assertThat(actual).containsExactly(4L, 5L).inOrder();
  }

  @Test
  public void testEmptyDataFrameWrite() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("long_col", DataTypes.LongType, false),
              DataTypes.createStructField("string_col", DataTypes.StringType, true),
            });

    Dataset<Row> df = spark.createDataFrame(Collections.emptyList(), schema);

    Map<String, String> props = connectionProperties(usePostgresSql);
    props.put("table", TestData.WRITE_TABLE_NAME);
    props.put("enablePartialRowUpdates", "true");

    // Get initial count to ensure no new rows are added
    long initialCount = spark.read().format("cloud-spanner").options(props).load().count();

    df.write().format("cloud-spanner").options(props).mode(SaveMode.Append).save();

    long finalCount = spark.read().format("cloud-spanner").options(props).load().count();

    assertEquals(
        "Writing an empty DataFrame should not change the row count", initialCount, finalCount);
  }

  @Test
  public void testUpsert() {
    // 1. Write initial data using unique keys.
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("long_col", DataTypes.LongType, false),
              DataTypes.createStructField("string_col", DataTypes.StringType, true),
            });
    List<Row> initialRows =
        Arrays.asList(
            RowFactory.create(201L, "original twenty-one"),
            RowFactory.create(202L, "original twenty-two"));
    Dataset<Row> initialDf = spark.createDataFrame(initialRows, schema);

    Map<String, String> props = connectionProperties(usePostgresSql);
    props.put("table", TestData.WRITE_TABLE_NAME);
    props.put("enablePartialRowUpdates", "true");

    initialDf.write().format("cloud-spanner").options(props).mode(SaveMode.Append).save();

    // 2. Write a second DataFrame to update one row and insert another.
    List<Row> newRows =
        Arrays.asList(
            RowFactory.create(201L, "new twenty-one"), // Update 201
            RowFactory.create(203L, "new twenty-three") // Insert 203
            );
    Dataset<Row> newDf = spark.createDataFrame(newRows, schema);

    newDf.write().format("cloud-spanner").options(props).mode(SaveMode.Append).save();

    // 3. Verify the final state of the rows involved in this test.
    Dataset<Row> finalDf =
        spark
            .read()
            .format("cloud-spanner")
            .options(props)
            .load()
            .filter("long_col IN (201, 202, 203)");

    assertEquals(3, finalDf.count());

    Map<Long, Row> finalRows =
        finalDf.collectAsList().stream()
            .collect(java.util.stream.Collectors.toMap(r -> r.getLong(0), r -> r));

    // Check that row 201 was updated.
    assertThat(finalRows.get(201L).getString(1)).isEqualTo("new twenty-one");
    // Check that row 202 was not touched.
    assertThat(finalRows.get(202L).getString(1)).isEqualTo("original twenty-two");
    // Check that row 203 was inserted.
    assertThat(finalRows.get(203L).getString(1)).isEqualTo("new twenty-three");
  }

  @Test
  public void testUpdateSetColumnToNull() {
    // 1. Write initial data with a non-null string value
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("long_col", DataTypes.LongType, false),
              DataTypes.createStructField("string_col", DataTypes.StringType, true),
            });
    List<Row> initialRows = Collections.singletonList(RowFactory.create(20L, "originalValue"));
    Dataset<Row> initialDf = spark.createDataFrame(initialRows, schema);

    Map<String, String> props = connectionProperties(usePostgresSql);
    props.put("table", TestData.WRITE_TABLE_NAME);
    props.put("enablePartialRowUpdates", "true");

    initialDf.write().format("cloud-spanner").options(props).mode(SaveMode.Append).save();

    // Verify initial data
    Dataset<Row> dfAfterInitialWrite =
        spark.read().format("cloud-spanner").options(props).load().filter("long_col = 20");
    assertEquals(1, dfAfterInitialWrite.count());
    assertThat(dfAfterInitialWrite.first().getString(1)).isEqualTo("originalValue");

    // 2. Update the existing row, setting string_col to null
    List<Row> updateRows = Collections.singletonList(RowFactory.create(20L, null));
    Dataset<Row> updateDf = spark.createDataFrame(updateRows, schema);

    updateDf.write().format("cloud-spanner").options(props).mode(SaveMode.Append).save();

    // 3. Verify that the string_col is now null
    Dataset<Row> dfAfterUpdate =
        spark.read().format("cloud-spanner").options(props).load().filter("long_col = 20");
    assertEquals(1, dfAfterUpdate.count());
    assertNull(dfAfterUpdate.first().get(1));
  }

  @Test
  public void testWrite() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("long_col", DataTypes.LongType, false),
              DataTypes.createStructField("string_col", DataTypes.StringType, true),
              DataTypes.createStructField("bool_col", DataTypes.BooleanType, true),
              DataTypes.createStructField("double_col", DataTypes.DoubleType, true),
              DataTypes.createStructField("timestamp_col", DataTypes.TimestampType, true),
              DataTypes.createStructField("date_col", DataTypes.DateType, true),
              DataTypes.createStructField("bytes_col", DataTypes.BinaryType, true),
              DataTypes.createStructField("numeric_col", DataTypes.createDecimalType(38, 9), true),
            });

    List<Row> rows =
        Arrays.asList(
            RowFactory.create(
                101L,
                "one",
                true,
                1.1,
                java.sql.Timestamp.valueOf("2023-01-01 10:10:10"),
                java.sql.Date.valueOf("2023-01-01"),
                new byte[] {1, 2, 3},
                new java.math.BigDecimal("123.456")),
            RowFactory.create(
                102L,
                "two",
                false,
                2.2,
                java.sql.Timestamp.valueOf("2023-02-02 20:20:20"),
                java.sql.Date.valueOf("2023-02-02"),
                new byte[] {4, 5, 6},
                new java.math.BigDecimal("789.012")));

    Dataset<Row> df = spark.createDataFrame(rows, schema);

    Map<String, String> props = connectionProperties(usePostgresSql);
    props.put("table", TestData.WRITE_TABLE_NAME);

    df.write().format("cloud-spanner").options(props).mode(SaveMode.Append).save();

    Dataset<Row> writtenDf =
        spark.read().format("cloud-spanner").options(props).load().filter("long_col IN (101, 102)");

    assertEquals(2, writtenDf.count());

    Map<Long, Row> writtenRows =
        writtenDf.collectAsList().stream()
            .collect(java.util.stream.Collectors.toMap(r -> r.getLong(0), r -> r));

    Row row1 = writtenRows.get(101L);
    assertThat(row1.getString(1)).isEqualTo("one");
    assertThat(row1.getBoolean(2)).isTrue();
    assertThat(row1.getDouble(3)).isEqualTo(1.1);
    assertThat(row1.getTimestamp(4)).isEqualTo(java.sql.Timestamp.valueOf("2023-01-01 10:10:10"));
    assertThat(row1.getDate(5)).isEqualTo(java.sql.Date.valueOf("2023-01-01"));
    assertThat(row1.<byte[]>getAs(6)).isEqualTo(new byte[] {1, 2, 3});
    assertThat(row1.getDecimal(7).compareTo(new java.math.BigDecimal("123.456"))).isEqualTo(0);

    Row row2 = writtenRows.get(102L);
    assertThat(row2.getString(1)).isEqualTo("two");
    assertThat(row2.getBoolean(2)).isFalse();
    assertThat(row2.getDouble(3)).isEqualTo(2.2);
    assertThat(row2.getTimestamp(4)).isEqualTo(java.sql.Timestamp.valueOf("2023-02-02 20:20:20"));
    assertThat(row2.getDate(5)).isEqualTo(java.sql.Date.valueOf("2023-02-02"));
    assertThat(row2.<byte[]>getAs(6)).isEqualTo(new byte[] {4, 5, 6});
    assertThat(row2.getDecimal(7).compareTo(new java.math.BigDecimal("789.012"))).isEqualTo(0);
  }

  @Test
  public void testArrayWrite() {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("long_col", DataTypes.LongType, false),
              DataTypes.createStructField("string_col", DataTypes.StringType, true),
              DataTypes.createStructField("bool_col", DataTypes.BooleanType, true),
              DataTypes.createStructField("double_col", DataTypes.DoubleType, true),
              DataTypes.createStructField("timestamp_col", DataTypes.TimestampType, true),
              DataTypes.createStructField("date_col", DataTypes.DateType, true),
              DataTypes.createStructField("bytes_col", DataTypes.BinaryType, true),
              DataTypes.createStructField("numeric_col", DataTypes.createDecimalType(38, 9), true),
              DataTypes.createStructField(
                  "long_array", DataTypes.createArrayType(DataTypes.LongType), true),
              DataTypes.createStructField(
                  "str_array", DataTypes.createArrayType(DataTypes.StringType), true),
              DataTypes.createStructField(
                  "boolean_array", DataTypes.createArrayType(DataTypes.BooleanType), true),
              DataTypes.createStructField(
                  "double_array", DataTypes.createArrayType(DataTypes.DoubleType), true),
              DataTypes.createStructField(
                  "timestamp_array", DataTypes.createArrayType(DataTypes.TimestampType), true),
              DataTypes.createStructField(
                  "date_array", DataTypes.createArrayType(DataTypes.DateType), true),
              DataTypes.createStructField(
                  "binary_array", DataTypes.createArrayType(DataTypes.BinaryType), true),
              DataTypes.createStructField(
                  "numeric_array",
                  DataTypes.createArrayType(DataTypes.createDecimalType(38, 9)),
                  true),
            });

    final Long[] testLongArray1 = new Long[] {1L, null, 3L};
    final long[] testLongArray2 = new long[] {4L, 5L, 6L};
    final Object[] testStrArray1 = new Object[] {"A", "B", null};
    final Object[] testStrArray2 = new Object[] {"C", "D"};
    final Boolean[] testBooleanArray1 = new Boolean[] {true, false, null};
    final boolean[] testBooleanArray2 = new boolean[] {false, true};
    final Double[] testDoubleArray1 = new Double[] {95.5, -10.88, null};
    final double[] testDoubleArray2 = new double[] {19.64, -213.44};
    final Timestamp[] testTimestamp1 = {
      java.sql.Timestamp.valueOf("2023-01-01 23:59:59"),
      java.sql.Timestamp.valueOf("2024-01-01 10:10:10"),
      null
    };
    final Timestamp[] testTimestamp2 = {
      java.sql.Timestamp.valueOf("2025-12-31 00:01:01"),
      java.sql.Timestamp.valueOf("2026-2-11 14:10:10")
    };
    final Date[] testDateArray1 = {
      java.sql.Date.valueOf("2023-01-01"), java.sql.Date.valueOf("2024-01-01"), null
    };
    final Date[] testDateArray2 = {
      java.sql.Date.valueOf("2025-01-01"), java.sql.Date.valueOf("2026-01-01")
    };
    final Object[] testBinaryArray1 = {new byte[] {1, 2, 3}, new byte[] {4, 5, 6}};
    final Object[] testBinaryArray2 = {new byte[] {7, 8, 9}, new byte[] {10, 11, 12}};

    final MathContext mc = new MathContext(38, RoundingMode.HALF_UP);
    final Object[] testDecimalArray1 = {
      new BigDecimal("123.456", mc).setScale(9, RoundingMode.HALF_UP),
      new BigDecimal("987.654", mc).setScale(9, RoundingMode.HALF_UP),
      null
    };
    final Object[] testDecimalArray2 = {
      new BigDecimal("135.791", mc).setScale(9, RoundingMode.HALF_UP),
      new BigDecimal("246.802", mc).setScale(9, RoundingMode.HALF_UP)
    };

    List<Row> rows =
        Arrays.asList(
            RowFactory.create(
                101L,
                "one",
                true,
                1.1,
                java.sql.Timestamp.valueOf("2023-01-01 10:10:10"),
                java.sql.Date.valueOf("2023-01-01"),
                new byte[] {1, 2, 3},
                new java.math.BigDecimal("123.456"),
                testLongArray1,
                testStrArray1,
                testBooleanArray1,
                testDoubleArray1,
                testTimestamp1,
                testDateArray1,
                testBinaryArray1,
                testDecimalArray1),
            RowFactory.create(
                102L,
                "two",
                false,
                2.2,
                java.sql.Timestamp.valueOf("2023-02-02 20:20:20"),
                java.sql.Date.valueOf("2023-02-02"),
                new byte[] {4, 5, 6},
                new java.math.BigDecimal("789.012"),
                testLongArray2,
                testStrArray2,
                testBooleanArray2,
                testDoubleArray2,
                testTimestamp2,
                testDateArray2,
                testBinaryArray2,
                testDecimalArray2));

    Dataset<Row> df = spark.createDataFrame(rows, schema);

    Map<String, String> props = connectionProperties(usePostgresSql);
    props.put("table", WRITE_ARRAY_TABLE_NAME);

    df.write().format("cloud-spanner").options(props).mode(SaveMode.Append).save();

    Dataset<Row> writtenDf =
        spark.read().format("cloud-spanner").options(props).load().filter("long_col IN (101, 102)");

    assertEquals(2, writtenDf.count());

    Map<Long, Row> writtenRows =
        writtenDf.collectAsList().stream()
            .collect(java.util.stream.Collectors.toMap(r -> r.getLong(0), r -> r));

    Row row1 = writtenRows.get(101L);
    assertThat(row1.getString(1)).isEqualTo("one");
    assertThat(row1.getBoolean(2)).isTrue();
    assertThat(row1.getDouble(3)).isEqualTo(1.1);
    assertThat(row1.getTimestamp(4)).isEqualTo(java.sql.Timestamp.valueOf("2023-01-01 10:10:10"));
    assertThat(row1.getDate(5)).isEqualTo(java.sql.Date.valueOf("2023-01-01"));
    assertThat(row1.<byte[]>getAs(6)).isEqualTo(new byte[] {1, 2, 3});
    assertThat(row1.getDecimal(7).compareTo(new java.math.BigDecimal("123.456"))).isEqualTo(0);
    assertArrayEquals(testLongArray1, rowToLongObjectArray(row1, 8));
    assertArrayEquals(testStrArray1, rowToStrObjectArray(row1, 9));
    assertArrayEquals(testBooleanArray1, rowToBooleanObjectArray(row1, 10));
    assertArrayEquals(testDoubleArray1, rowToDoubleObjectArray(row1, 11));
    assertArrayEquals(testTimestamp1, rowToTimestampArray(row1, 12));
    assertArrayEquals(testDateArray1, rowToDateArray(row1, 13));
    assertTrue(java.util.Arrays.deepEquals(testBinaryArray1, rowToByteArray(row1, 14)));
    assertArrayEquals(testDecimalArray1, rowToDecimalArray(row1, 15));

    Row row2 = writtenRows.get(102L);
    assertThat(row2.getString(1)).isEqualTo("two");
    assertThat(row2.getBoolean(2)).isFalse();
    assertThat(row2.getDouble(3)).isEqualTo(2.2);
    assertThat(row2.getTimestamp(4)).isEqualTo(java.sql.Timestamp.valueOf("2023-02-02 20:20:20"));
    assertThat(row2.getDate(5)).isEqualTo(java.sql.Date.valueOf("2023-02-02"));
    assertThat(row2.<byte[]>getAs(6)).isEqualTo(new byte[] {4, 5, 6});
    assertThat(row2.getDecimal(7).compareTo(new java.math.BigDecimal("789.012"))).isEqualTo(0);
    assertArrayEquals(testLongArray2, rowToLongArray(row2, 8));
    assertArrayEquals(testStrArray2, rowToStrObjectArray(row2, 9));
    assertArrayEquals(testBooleanArray2, rowToBooleanArray(row2, 10));
    assertArrayEquals(testDoubleArray2, rowToDoubleArray(row2, 11), 0.01);
    assertArrayEquals(testTimestamp2, rowToTimestampArray(row2, 12));
    assertArrayEquals(testDateArray2, rowToDateArray(row2, 13));
    assertTrue(java.util.Arrays.deepEquals(testBinaryArray2, rowToByteArray(row2, 14)));
    assertArrayEquals(testDecimalArray2, rowToDecimalArray(row2, 15));
  }

  private long[] rowToLongArray(Row row, int index) {
    final List<Long> actualLongList = row.getList(index);
    return actualLongList.stream().mapToLong(l -> l).toArray();
  }

  private Long[] rowToLongObjectArray(Row row, int index) {
    final List<Long> actualLongList = row.getList(index);
    return actualLongList.toArray(new Long[0]);
  }

  private Object[] rowToStrObjectArray(Row row, int index) {
    final List<String> actualList = row.getList(index);
    return actualList.toArray();
  }

  private boolean[] rowToBooleanArray(Row row, int index) {
    final List<Boolean> actualList = row.getList(index);
    boolean[] booleanArray = new boolean[actualList.size()];

    for (int i = 0; i < actualList.size(); i++) {
      booleanArray[i] = actualList.get(i);
    }
    return booleanArray;
  }

  private Boolean[] rowToBooleanObjectArray(Row row, int index) {
    final List<Boolean> actualList = row.getList(index);
    return actualList.toArray(new Boolean[0]);
  }

  private double[] rowToDoubleArray(Row row, int index) {
    final List<Double> actualList = row.getList(index);
    return actualList.stream().mapToDouble(d -> d).toArray();
  }

  private Double[] rowToDoubleObjectArray(Row row, int index) {
    final List<Double> actualList = row.getList(index);
    return actualList.toArray(new Double[0]);
  }

  private Timestamp[] rowToTimestampArray(Row row, int index) {
    final List<Timestamp> actualList = row.getList(index);
    return actualList.toArray(new Timestamp[0]);
  }

  private Date[] rowToDateArray(Row row, int index) {
    final List<Date> actualList = row.getList(index);
    return actualList.toArray(new Date[0]);
  }

  private Object[] rowToByteArray(Row row, int index) {
    final List<byte[]> actualList = row.<byte[]>getList(index);
    return actualList.toArray();
  }

  private BigDecimal[] rowToDecimalArray(Row row, int index) {
    final List<BigDecimal> actualList = row.getList(index);
    return actualList.toArray(new BigDecimal[0]);
  }
}
