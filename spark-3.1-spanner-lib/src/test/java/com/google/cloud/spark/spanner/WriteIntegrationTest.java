package com.google.cloud.spark.spanner;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
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

public class WriteIntegrationTest extends SparkSpannerIntegrationTestBase {

  private static final String WRITE_TABLE_NAME = "writeTestTable";

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
            });

    List<Row> rows =
        Collections.singletonList(RowFactory.create(3L, null, null, null, null, null, null, null));

    Dataset<Row> df = spark.createDataFrame(rows, schema);

    Map<String, String> props = connectionProperties();
    props.put("table", WRITE_TABLE_NAME);

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

    Map<String, String> props = connectionProperties();
    props.put("table", WRITE_TABLE_NAME);
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

    Map<String, String> props = connectionProperties();
    props.put("table", WRITE_TABLE_NAME);
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

    Map<String, String> props = connectionProperties();
    props.put("table", WRITE_TABLE_NAME);
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

    Map<String, String> props = connectionProperties();
    props.put("table", WRITE_TABLE_NAME);
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

    Map<String, String> props = connectionProperties();
    props.put("table", WRITE_TABLE_NAME);

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
}
