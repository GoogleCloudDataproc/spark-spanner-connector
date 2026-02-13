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

package com.google.cloud.spark.spanner;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.function.BiConsumer;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
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
  private static final MathContext mc = new MathContext(38, RoundingMode.HALF_UP);
  private static final java.math.BigDecimal jbd =
      new java.math.BigDecimal("135.791", mc).setScale(9, RoundingMode.HALF_UP);
  private static final scala.math.BigDecimal bd = scala.math.BigDecimal$.MODULE$.apply(jbd);
  private static final Decimal decimal = new Decimal().set(bd);

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
            {"dt", DataTypes.DateType, 19723, Value.date(Date.fromYearMonthDay(2024, 1, 1))},
            {"decimal", new DecimalType(38, 9), decimal, Value.numeric(jbd)}
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
      else if (sparkType instanceof DecimalType)
        when(row.getDecimal(0, 38, 9)).thenReturn((Decimal) mockValue);

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
            {"decimal", new DecimalType(38, 9), Value.numeric(null)},
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
            {"date_array", DataTypes.createArrayType(DataTypes.DateType), Value.dateArray(null)},
            {
              "decimal_array",
              DataTypes.createArrayType(new DecimalType(38, 9)),
              Value.numericArray(null)
            }
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
              new Long[] {1L, 2L, null},
              Value.int64Array(Arrays.asList(1L, 2L, null)),
              (BiConsumer<ArrayData, Object>)
                  (ad, d) ->
                      setupArrayMock(
                          ad, (Long[]) d, (i, val) -> when(ad.getLong(i)).thenReturn((Long) val))
            },

            // 2. String Array
            {
              "str_array",
              DataTypes.StringType,
              new String[] {"A", "B", null},
              Value.stringArray(Arrays.asList("A", "B", null)),
              (BiConsumer<ArrayData, Object>)
                  (ad, d) ->
                      setupArrayMock(
                          ad,
                          (String[]) d,
                          (i, val) ->
                              when(ad.getUTF8String(i))
                                  .thenReturn(UTF8String.fromString((String) val)))
            },

            // 3. Boolean Array
            {
              "boolean_array",
              DataTypes.BooleanType,
              new Boolean[] {true, false, null},
              Value.boolArray(Arrays.asList(new Boolean[] {true, false, null})),
              (BiConsumer<ArrayData, Object>)
                  (ad, d) ->
                      setupArrayMock(
                          ad,
                          (Boolean[]) d,
                          (i, val) -> when(ad.getBoolean(i)).thenReturn((Boolean) val))
            },

            // 4. Double Array
            {
              "double_array",
              DataTypes.DoubleType,
              new Double[] {95.5, -10.88, null},
              Value.float64Array(Arrays.asList(new Double[] {95.5, -10.88, null})),
              (BiConsumer<ArrayData, Object>)
                  (ad, d) ->
                      setupArrayMock(
                          ad,
                          (Double[]) d,
                          (i, val) -> when(ad.getDouble(i)).thenReturn((Double) val))
            },

            // 5. Binary Array (Array[Byte])
            {
              "binary_array",
              DataTypes.BinaryType,
              new byte[][] {new byte[] {95, 10, 127}, null},
              Value.bytesArray(Arrays.asList(ByteArray.copyFrom(new byte[] {95, 10, 127}), null)),
              (BiConsumer<ArrayData, Object>)
                  (ad, d) ->
                      setupArrayMock(
                          ad,
                          (byte[][]) d,
                          (i, val) -> when(ad.getBinary(i)).thenReturn((byte[]) val))
            },

            // 6. Timestamp Array (Stored as microseconds in Spark)
            {
              "timestamp_array",
              DataTypes.TimestampType,
              new Long[] {1704067200000000L, null},
              Value.timestampArray(
                  Arrays.asList(Timestamp.ofTimeMicroseconds(1704067200000000L), null)),
              (BiConsumer<ArrayData, Object>)
                  (ad, d) ->
                      setupArrayMock(
                          ad, (Long[]) d, (i, val) -> when(ad.getLong(i)).thenReturn((Long) val))
            },

            // 7. Date Array (Stored as epoch days in Spark)
            {
              "date_array",
              DataTypes.DateType,
              new Integer[] {18628, null}, // Epoch days
              Value.dateArray(Arrays.asList(Date.fromYearMonthDay(2021, 1, 1), null)),
              (BiConsumer<ArrayData, Object>)
                  (ad, d) ->
                      setupArrayMock(
                          ad,
                          (Integer[]) d,
                          (i, val) -> when(ad.getInt(i)).thenReturn((Integer) val))
            },

            // 8. Decimal Array
            {
              "decimal_array",
              new DecimalType(38, 9),
              new Decimal[] {decimal},
              Value.numericArray(Collections.singletonList(jbd)),
              (BiConsumer<ArrayData, Object>)
                  (ad, d) ->
                      setupArrayMock(
                          ad,
                          (Decimal[]) d,
                          (i, val) -> when(ad.getDecimal(i, 38, 9)).thenReturn((Decimal) val))
            }
          });
    }

    private static void setupArrayMock(
        ArrayData ad, Object[] data, BiConsumer<Integer, Object> getterStubber) {
      when(ad.numElements()).thenReturn(data.length);
      for (int i = 0; i < data.length; i++) {
        if (data[i] == null) {
          when(ad.isNullAt(i)).thenReturn(true);
        } else {
          when(ad.isNullAt(i)).thenReturn(false);
          getterStubber.accept(i, data[i]);
        }
      }
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
