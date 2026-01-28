package com.google.cloud.spark.spanner;

import static org.mockito.Mockito.*;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class SpannerWriterUtilsTest {

  private final String TABLE_NAME = "test_table";
  private static final byte[] BYTE_DATA = {95, -10, 127};

  // Parameters for each test case: [ColumnName, DataType, MockValue, ExpectedSpannerValue]
  @Parameters(name = "Testing {0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          {"long", DataTypes.LongType, 100L, Value.int64(100L)},
          {"string", DataTypes.StringType, "Hello", Value.string("Hello")},
          {"boolean", DataTypes.BooleanType, true, Value.bool(true)},
          {"double", DataTypes.DoubleType, 95.5, Value.float64(95.5)},
          {"binary", DataTypes.BinaryType, BYTE_DATA, Value.bytes(ByteArray.copyFrom(BYTE_DATA))}
        });
  }

  @Parameter(0)
  public String fieldName;

  @Parameter(1)
  public DataType sparkType;

  @Parameter(2)
  public Object mockValue;

  @Parameter(3)
  public Value expectedValue;

  @Test
  public void testScalarConversion() {
    // 1. Setup single-column schema for this parameter set
    StructType schema = new StructType().add(fieldName, sparkType);

    // 2. Mock InternalRow based on the current parameter's type
    InternalRow row = mock(InternalRow.class);
    when(row.isNullAt(0)).thenReturn(false);

    // Setup specific getter based on type
    if (sparkType == DataTypes.LongType) when(row.getLong(0)).thenReturn((Long) mockValue);
    else if (sparkType == DataTypes.StringType)
      when(row.getString(0)).thenReturn((String) mockValue);
    else if (sparkType == DataTypes.BooleanType)
      when(row.getBoolean(0)).thenReturn((Boolean) mockValue);
    else if (sparkType == DataTypes.DoubleType)
      when(row.getDouble(0)).thenReturn((Double) mockValue);
    else if (sparkType == DataTypes.BinaryType)
      when(row.getBinary(0)).thenReturn((byte[]) mockValue);

    // 3. Execute
    com.google.cloud.spanner.Mutation mutation =
        SpannerWriterUtils.internalRowToMutation(TABLE_NAME, row, schema);

    // 4. Verify the mapped value
    Assert.assertEquals(
        "Value mismatch for column: " + fieldName, expectedValue, mutation.asMap().get(fieldName));
  }

  @Test
  public void testNullHandling() {
    StructType schema =
        new StructType()
            .add("long", DataTypes.LongType)
            .add("string", DataTypes.StringType)
            .add("boolean", DataTypes.BooleanType)
            .add("double", DataTypes.DoubleType)
            .add("binary", DataTypes.BinaryType)
            .add("timestamp", DataTypes.TimestampType)
            .add("date", DataTypes.DateType)
            .add("long_array", DataTypes.createArrayType(DataTypes.LongType))
            .add("str_array", DataTypes.createArrayType(DataTypes.StringType))
            .add("boolean_array", DataTypes.createArrayType(DataTypes.BooleanType))
            .add("double_array", DataTypes.createArrayType(DataTypes.DoubleType))
            .add("binary_array", DataTypes.createArrayType(DataTypes.BinaryType))
            .add("timestamp_array", DataTypes.createArrayType(DataTypes.TimestampType))
            .add("date_array", DataTypes.createArrayType(DataTypes.DateType));
    InternalRow row = mock(InternalRow.class);

    // Simulate a null value in Spark
    when(row.isNullAt(anyInt())).thenReturn(true);

    Mutation mutation = SpannerWriterUtils.internalRowToMutation(TABLE_NAME, row, schema);

    Assert.assertEquals(Value.int64(null), mutation.asMap().get("long"));
    Assert.assertEquals(Value.string(null), mutation.asMap().get("string"));
    Assert.assertEquals(Value.bool(null), mutation.asMap().get("boolean"));
    Assert.assertEquals(Value.float64(null), mutation.asMap().get("double"));
    Assert.assertEquals(Value.bytes(null), mutation.asMap().get("binary"));
    Assert.assertEquals(Value.timestamp(null), mutation.asMap().get("timestamp"));
    Assert.assertEquals(Value.date(null), mutation.asMap().get("date"));
    Assert.assertEquals(Value.int64Array((long[]) null), mutation.asMap().get("long_array"));
    Assert.assertEquals(Value.stringArray(null), mutation.asMap().get("str_array"));
    Assert.assertEquals(Value.boolArray((boolean[]) null), mutation.asMap().get("boolean_array"));
    Assert.assertEquals(Value.float64Array((double[]) null), mutation.asMap().get("double_array"));
    Assert.assertEquals(Value.bytesArray(null), mutation.asMap().get("binary_array"));
    Assert.assertEquals(Value.timestampArray(null), mutation.asMap().get("timestamp_array"));
    Assert.assertEquals(Value.dateArray(null), mutation.asMap().get("date_array"));
  }

  @Test
  public void testTimestampAndDate() {
    StructType schema =
        new StructType().add("ts", DataTypes.TimestampType).add("dt", DataTypes.DateType);

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

    Object[] stringObjectData = {"A", "B"};
    Iterable<String> stringData =
        java.util.Arrays.stream(stringObjectData)
            .map(item -> item == null ? null : item.toString())
            .collect(java.util.stream.Collectors.toList());
    when(row.isNullAt(1)).thenReturn(false);
    when(row.getArray(1)).thenReturn(arrayData);
    when(arrayData.toObjectArray(DataTypes.StringType)).thenReturn(stringObjectData);

    boolean[] booleanData = {true, false};
    when(row.isNullAt(2)).thenReturn(false);
    when(row.getArray(2)).thenReturn(arrayData);
    when(arrayData.toBooleanArray()).thenReturn(booleanData);

    double[] doubleData = {95.5, -10.88};
    when(row.isNullAt(3)).thenReturn(false);
    when(row.getArray(3)).thenReturn(arrayData);
    when(arrayData.toDoubleArray()).thenReturn(doubleData);

    byte[] byteData = {95, 10, 127};
    Object[] byteObjectData = new Object[] {byteData, null};
    when(row.isNullAt(4)).thenReturn(false);
    when(row.getArray(4)).thenReturn(arrayData);
    when(arrayData.toObjectArray(DataTypes.BinaryType)).thenReturn(byteObjectData);

    // Execute
    Mutation mutation = SpannerWriterUtils.internalRowToMutation(TABLE_NAME, row, schema);

    Assert.assertEquals(Value.int64Array(longData), mutation.asMap().get("long_array"));
    Assert.assertEquals(Value.stringArray(stringData), mutation.asMap().get("str_array"));
    Assert.assertEquals(Value.boolArray(booleanData), mutation.asMap().get("boolean_array"));
    Assert.assertEquals(Value.float64Array(doubleData), mutation.asMap().get("double_array"));

    // Construct expected Spanner Value
    List<ByteArray> expectedByteList = Arrays.asList(ByteArray.copyFrom(byteData), null);
    Assert.assertEquals(Value.bytesArray(expectedByteList), mutation.asMap().get("binary_array"));
  }

  @Test
  public void testTimestampAndDateArrays() {
    StructType schema =
        new StructType()
            .add("timestamp_array", DataTypes.createArrayType(DataTypes.TimestampType))
            .add("date_array", DataTypes.createArrayType(DataTypes.DateType));
    // Note: As of 2026, ensure your logic accounts for modern epoch handling
    long[] micros = {1704067200000000L}; // 2024-01-01 00:00:00
    List<Timestamp> timestamps = new ArrayList<>(micros.length);
    for (long micro : micros) {
      timestamps.add(Timestamp.ofTimeMicroseconds(micro));
    }
    int[] days = {19723}; // 2024-01-01 in epoch days
    List<Date> dates = new ArrayList<>(days.length);
    dates.add(Date.fromYearMonthDay(2024, 1, 1));

    InternalRow row = mock(InternalRow.class);
    ArrayData arrayData = mock(ArrayData.class);

    when(row.isNullAt(0)).thenReturn(false);
    when(row.getArray(0)).thenReturn(arrayData);
    when(arrayData.toLongArray()).thenReturn(micros);
    when(row.isNullAt(1)).thenReturn(false);
    when(row.getArray(1)).thenReturn(arrayData);
    when(arrayData.toIntArray()).thenReturn(days);

    Mutation mutation = SpannerWriterUtils.internalRowToMutation(TABLE_NAME, row, schema);
    Assert.assertEquals(Value.timestampArray(timestamps), mutation.asMap().get("timestamp_array"));
    Assert.assertEquals(Value.dateArray(dates), mutation.asMap().get("date_array"));
  }
}
