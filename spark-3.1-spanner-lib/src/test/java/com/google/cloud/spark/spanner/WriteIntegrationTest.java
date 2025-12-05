package com.google.cloud.spark.spanner;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
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
                1L,
                "one",
                true,
                1.1,
                java.sql.Timestamp.valueOf("2023-01-01 10:10:10"),
                java.sql.Date.valueOf("2023-01-01"),
                new byte[] {1, 2, 3},
                new java.math.BigDecimal("123.456")),
            RowFactory.create(
                2L,
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

    Dataset<Row> writtenDf = spark.read().format("cloud-spanner").options(props).load();

    assertEquals(2, writtenDf.count());

    List<Row> writtenRows = writtenDf.collectAsList();
    assertThat(writtenRows.get(0).getLong(0)).isEqualTo(1L);
    assertThat(writtenRows.get(0).getString(1)).isEqualTo("one");
    assertThat(writtenRows.get(0).getBoolean(2)).isTrue();
    assertThat(writtenRows.get(0).getDouble(3)).isEqualTo(1.1);
    assertThat(writtenRows.get(0).getTimestamp(4))
        .isEqualTo(java.sql.Timestamp.valueOf("2023-01-01 10:10:10"));
    assertThat(writtenRows.get(0).getDate(5)).isEqualTo(java.sql.Date.valueOf("2023-01-01"));
    assertThat(writtenRows.get(0).<byte[]>getAs(6)).isEqualTo(new byte[] {1, 2, 3});
    assertThat(writtenRows.get(0).getDecimal(7)).isEqualTo(new java.math.BigDecimal("123.456"));

    assertThat(writtenRows.get(1).getLong(0)).isEqualTo(2L);
    assertThat(writtenRows.get(1).getString(1)).isEqualTo("two");
    assertThat(writtenRows.get(1).getBoolean(2)).isFalse();
    assertThat(writtenRows.get(1).getDouble(3)).isEqualTo(2.2);
    assertThat(writtenRows.get(1).getTimestamp(4))
        .isEqualTo(java.sql.Timestamp.valueOf("2023-02-02 20:20:20"));
    assertThat(writtenRows.get(1).getDate(5)).isEqualTo(java.sql.Date.valueOf("2023-02-02"));
    assertThat(writtenRows.get(1).<byte[]>getAs(6)).isEqualTo(new byte[] {4, 5, 6});
    assertThat(writtenRows.get(1).getDecimal(7)).isEqualTo(new java.math.BigDecimal("789.012"));
  }
}
