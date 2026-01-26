package com.google.cloud.spark.spanner;

import static org.mockito.Mockito.*;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Test;

public class SpannerWriterUtilsTest {

  private final String TABLE_NAME = "test_table";

  @Test
  public void testScalarTypes() {
    // Define Schema
    StructType schema =
        new StructType()
            .add("id", DataTypes.LongType)
            .add("name", DataTypes.StringType)
            .add("active", DataTypes.BooleanType)
            .add("score", DataTypes.DoubleType);

    // Mock InternalRow
    InternalRow row = mock(InternalRow.class);
    when(row.isNullAt(0)).thenReturn(false);
    when(row.getLong(0)).thenReturn(100L);
    when(row.isNullAt(1)).thenReturn(false);
    when(row.getString(1)).thenReturn("Hello");
    when(row.isNullAt(2)).thenReturn(false);
    when(row.getBoolean(2)).thenReturn(true);
    when(row.isNullAt(3)).thenReturn(false);
    when(row.getDouble(3)).thenReturn(95.5);

    Mutation mutation = SpannerWriterUtils.internalRowToMutation(TABLE_NAME, row, schema);
    Map<String, Value> values = mutation.asMap();

    Assert.assertEquals(Value.int64(100L), values.get("id"));
    Assert.assertEquals(Value.string("Hello"), values.get("name"));
    Assert.assertEquals(Value.bool(true), values.get("active"));
    Assert.assertEquals(Value.float64(95.5), values.get("score"));
  }

  @Test
  public void testNullHandling() {
    StructType schema = new StructType().add("nullable_col", DataTypes.StringType);
    InternalRow row = mock(InternalRow.class);

    // Simulate a null value in Spark
    when(row.isNullAt(0)).thenReturn(true);

    Mutation mutation = SpannerWriterUtils.internalRowToMutation(TABLE_NAME, row, schema);

    Assert.assertEquals(Value.string(null), mutation.asMap().get("nullable_col"));
  }

  @Test
  public void testTimestampAndDate() {
    StructType schema =
        new StructType().add("ts", DataTypes.TimestampType).add("dt", DataTypes.DateType);

    // Note: As of 2026, ensure your logic accounts for modern epoch handling
    long micros = 1704067200000000L; // 2024-01-01 00:00:00
    int days = 19723; // 2024-01-01 in epoch days

    InternalRow row = mock(InternalRow.class);
    when(row.isNullAt(0)).thenReturn(false);
    when(row.getLong(0)).thenReturn(micros);
    when(row.isNullAt(1)).thenReturn(false);
    when(row.getInt(1)).thenReturn(days);

    Mutation mutation = SpannerWriterUtils.internalRowToMutation(TABLE_NAME, row, schema);

    Assert.assertEquals(
        Value.timestamp(Timestamp.ofTimeMicroseconds(micros)), mutation.asMap().get("ts"));
    Assert.assertEquals(Value.date(Date.fromYearMonthDay(2024, 1, 1)), mutation.asMap().get("dt"));
  }

  @Test
  public void testScalarArrayConversion() {
    StructType schema =
        new StructType()
            .add("long_array", DataTypes.createArrayType(DataTypes.LongType))
            .add("str_array", DataTypes.createArrayType(DataTypes.StringType))
            .add("boolean_array", DataTypes.createArrayType(DataTypes.BooleanType))
            .add("double_array", DataTypes.createArrayType(DataTypes.DoubleType))
            .add("binary_array", DataTypes.createArrayType(DataTypes.BinaryType));

    InternalRow row = mock(InternalRow.class);
    ArrayData arrayData = mock(ArrayData.class);

    long[] longData = {1L, 2L, 3L};
    when(row.isNullAt(0)).thenReturn(false);
    when(row.getArray(0)).thenReturn(arrayData);
    when(arrayData.toLongArray()).thenReturn(longData);

    String[] stringData = {"A", "B"};
    when(row.isNullAt(1)).thenReturn(false);
    when(row.getArray(1)).thenReturn(arrayData);
    when(arrayData.toObjectArray(isA(DataType.class))).thenReturn(stringData);

    boolean[] booleanData = {true, false};
    when(row.isNullAt(2)).thenReturn(false);
    when(row.getArray(2)).thenReturn(arrayData);
    when(arrayData.toBooleanArray()).thenReturn(booleanData);

    double[] doubleData = {95.5, -10.88};
    when(row.isNullAt(3)).thenReturn(false);
    when(row.getArray(3)).thenReturn(arrayData);
    when(arrayData.toDoubleArray()).thenReturn(doubleData);

    byte[] byteData = {95, -10, 127};
    when(row.isNullAt(4)).thenReturn(false);
    when(row.getArray(4)).thenReturn(arrayData);
    when(arrayData.toByteArray()).thenReturn(byteData);

    Mutation mutation = SpannerWriterUtils.internalRowToMutation(TABLE_NAME, row, schema);

    Assert.assertEquals(Value.int64Array(longData), mutation.asMap().get("long_array"));
    Assert.assertEquals(
        Value.stringArray(Arrays.asList(stringData)), mutation.asMap().get("str_array"));
    Assert.assertEquals(Value.boolArray(booleanData), mutation.asMap().get("boolean_array"));
    Assert.assertEquals(Value.float64Array(doubleData), mutation.asMap().get("double_array"));
    Assert.assertEquals(
        Value.bytesArray(Collections.singletonList(ByteArray.copyFrom(byteData))),
        mutation.asMap().get("binary_array"));
  }
}
