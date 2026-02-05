package com.google.cloud.spark.spanner;

import static org.mockito.Mockito.*;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.function.BiConsumer;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Enclosed.class)
public class SpannerWriterUtilsTest {
  private static final String TABLE_NAME = "test_table";

  @RunWith(Parameterized.class)
  public static class ScalarTests {
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
            {"binary", DataTypes.BinaryType, BYTE_DATA, Value.bytes(ByteArray.copyFrom(BYTE_DATA))},
            {
              "ts",
              DataTypes.TimestampType,
              1704067200000000L,
              Value.timestamp(Timestamp.ofTimeMicroseconds(1704067200000000L))
            },
            {"dt", DataTypes.DateType, 19723, Value.date(Date.fromYearMonthDay(2024, 1, 1))}
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
      else if (sparkType == DataTypes.TimestampType)
        when(row.getLong(0)).thenReturn((long) mockValue);
      else if (sparkType == DataTypes.DateType) when(row.getInt(0)).thenReturn((int) mockValue);

      // 3. Execute
      com.google.cloud.spanner.Mutation mutation =
          SpannerWriterUtils.internalRowToMutation(TABLE_NAME, row, schema);

      // 4. Verify the mapped value
      Assert.assertEquals(
          "Value mismatch for column: " + fieldName,
          expectedValue,
          mutation.asMap().get(fieldName));
    }
  }

  @RunWith(Parameterized.class)
  public static class NullTests {

    // Parameters for each test case: [ColumnName, DataType, ExpectedSpannerValue]
    @Parameters(name = "Testing {0}")
    public static Collection<Object[]> data() {
      return Arrays.asList(
          new Object[][] {
            {"long", DataTypes.LongType, Value.int64(null)},
            {"string", DataTypes.StringType, Value.string(null)},
            {"boolean", DataTypes.BooleanType, Value.bool(null)},
            {"double", DataTypes.DoubleType, Value.float64(null)},
            {"binary", DataTypes.BinaryType, Value.bytes(null)},
            {"timestamp", DataTypes.TimestampType, Value.timestamp(null)},
            {"date", DataTypes.DateType, Value.date(null)},
            {
              "long_array",
              DataTypes.createArrayType(DataTypes.LongType),
              Value.int64Array((long[]) null)
            },
            {"str_array", DataTypes.createArrayType(DataTypes.StringType), Value.stringArray(null)},
            {
              "boolean_array",
              DataTypes.createArrayType(DataTypes.BooleanType),
              Value.boolArray((boolean[]) null)
            },
            {
              "double_array",
              DataTypes.createArrayType(DataTypes.DoubleType),
              Value.float64Array((double[]) null)
            },
            {
              "binary_array",
              DataTypes.createArrayType(DataTypes.BinaryType),
              Value.bytesArray(null)
            },
            {
              "timestamp_array",
              DataTypes.createArrayType(DataTypes.TimestampType),
              Value.timestampArray(null)
            },
            {"date_array", DataTypes.createArrayType(DataTypes.DateType), Value.dateArray(null)}
          });
    }

    @Parameter(0)
    public String fieldName;

    @Parameter(1)
    public DataType sparkType;

    @Parameter(2)
    public Value expectedValue;

    @Test
    public void testNullHandling() {
      // 1. Setup single-column schema for this parameter set
      StructType schema = new StructType().add(fieldName, sparkType);

      // 2. Mock InternalRow based on the current parameter's type
      InternalRow row = mock(InternalRow.class);

      // Simulate a null value in Spark
      when(row.isNullAt(0)).thenReturn(true);

      // 2. Execute
      Mutation mutation = SpannerWriterUtils.internalRowToMutation(TABLE_NAME, row, schema);

      // 4. Verify the mapped value
      Assert.assertEquals(expectedValue, mutation.asMap().get(fieldName));
    }
  }

  @RunWith(Parameterized.class)
  public static class ScalarArrayTests {

    private final String colName;
    private final DataType sparkType;
    private final Object inputData;
    private final Value expectedValue;
    private final BiConsumer<ArrayData, Object> mockSetup;

    public ScalarArrayTests(
        String colName,
        DataType sparkType,
        Object inputData,
        Value expectedValue,
        BiConsumer<ArrayData, Object> mockSetup) {
      this.colName = colName;
      this.sparkType = sparkType;
      this.inputData = inputData;
      this.expectedValue = expectedValue;
      this.mockSetup = mockSetup;
    }

    @Parameters(name = "{0}")
    public static Collection<Object[]> data() {
      return Arrays.asList(
          new Object[][] {
            // 1. Long Array
            {
              "long_array",
              DataTypes.LongType,
              new long[] {1L, 2L, 3L},
              Value.int64Array(new long[] {1L, 2L, 3L}),
              (BiConsumer<ArrayData, Object>)
                  (ad, d) -> when(ad.toLongArray()).thenReturn((long[]) d)
            },

            // 2. String Array
            {
              "str_array",
              DataTypes.StringType,
              new Object[] {"A", "B"},
              Value.stringArray(Arrays.asList("A", "B")),
              (BiConsumer<ArrayData, Object>)
                  (ad, d) -> when(ad.toObjectArray(DataTypes.StringType)).thenReturn((Object[]) d)
            },

            // 3. Boolean Array
            {
              "boolean_array",
              DataTypes.BooleanType,
              new boolean[] {true, false},
              Value.boolArray(new boolean[] {true, false}),
              (BiConsumer<ArrayData, Object>)
                  (ad, d) -> when(ad.toBooleanArray()).thenReturn((boolean[]) d)
            },

            // 4. Double Array
            {
              "double_array",
              DataTypes.DoubleType,
              new double[] {95.5, -10.88},
              Value.float64Array(new double[] {95.5, -10.88}),
              (BiConsumer<ArrayData, Object>)
                  (ad, d) -> when(ad.toDoubleArray()).thenReturn((double[]) d)
            },

            // 5. Binary Array (Array[Byte])
            {
              "binary_array",
              DataTypes.BinaryType,
              new Object[] {new byte[] {95, 10, 127}, null},
              Value.bytesArray(Arrays.asList(ByteArray.copyFrom(new byte[] {95, 10, 127}), null)),
              (BiConsumer<ArrayData, Object>)
                  (ad, d) -> when(ad.toObjectArray(DataTypes.BinaryType)).thenReturn((Object[]) d)
            },

            // 6. Timestamp Array (Stored as microseconds in Spark)
            {
              "timestamp_array",
              DataTypes.TimestampType,
              new long[] {1704067200000000L},
              Value.timestampArray(
                  Collections.singletonList(Timestamp.ofTimeMicroseconds(1704067200000000L))),
              (BiConsumer<ArrayData, Object>)
                  (ad, d) -> when(ad.toLongArray()).thenReturn((long[]) d)
            },

            // 7. Date Array (Stored as epoch days in Spark)
            {
              "date_array",
              DataTypes.DateType,
              new int[] {19723},
              Value.dateArray(Collections.singletonList(Date.fromYearMonthDay(2024, 1, 1))),
              (BiConsumer<ArrayData, Object>) (ad, d) -> when(ad.toIntArray()).thenReturn((int[]) d)
            }
          });
    }

    @Test
    public void testArrayConversion() {
      String TABLE_NAME = "test_table";
      StructType schema = new StructType().add(colName, DataTypes.createArrayType(sparkType));
      InternalRow row = mock(InternalRow.class);
      ArrayData arrayData = mock(ArrayData.class);

      when(row.isNullAt(0)).thenReturn(false);
      when(row.getArray(0)).thenReturn(arrayData);

      // Executes the specific stubbing (e.g., ad.toLongArray() or ad.toIntArray())
      mockSetup.accept(arrayData, inputData);

      Mutation mutation = SpannerWriterUtils.internalRowToMutation(TABLE_NAME, row, schema);

      Assert.assertEquals(
          "Failure on type: " + colName, expectedValue, mutation.asMap().get(colName));
    }
  }
}
