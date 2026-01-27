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

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import java.math.BigDecimal;
import java.util.*;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.*;

public class SpannerWriterUtils {

  // Define a functional interface that handles a specific type
  @FunctionalInterface
  private interface FieldConverter {
    Value convert(InternalRow row, int index, DataType type);
  }

  // Create a Registry to map Spark DataTypes to Converters
  private static final Map<DataType, FieldConverter> TYPE_CONVERTERS = new HashMap<>();
  private static final Map<DataType, FieldConverter> ARRAY_TYPE_CONVERTERS = new HashMap<>();

  static {
    // Long
    TYPE_CONVERTERS.put(
        DataTypes.LongType,
        (row, i, type) -> row.isNullAt(i) ? Value.int64(null) : Value.int64(row.getLong(i)));

    // String
    TYPE_CONVERTERS.put(
        DataTypes.StringType,
        (row, i, type) -> row.isNullAt(i) ? Value.string(null) : Value.string(row.getString(i)));

    // Boolean
    TYPE_CONVERTERS.put(
        DataTypes.BooleanType,
        (row, i, type) -> row.isNullAt(i) ? Value.bool(null) : Value.bool(row.getBoolean(i)));

    // Double
    TYPE_CONVERTERS.put(
        DataTypes.DoubleType,
        (row, i, type) -> row.isNullAt(i) ? Value.float64(null) : Value.float64(row.getDouble(i)));

    // Binary
    TYPE_CONVERTERS.put(
        DataTypes.BinaryType,
        (row, i, type) ->
            row.isNullAt(i)
                ? Value.bytes(null)
                : Value.bytes(ByteArray.copyFrom(row.getBinary(i))));

    // Timestamp
    TYPE_CONVERTERS.put(
        DataTypes.TimestampType,
        (row, i, type) -> {
          if (row.isNullAt(i)) return Value.timestamp(null);
          return Value.timestamp(Timestamp.ofTimeMicroseconds(row.getLong(i)));
        });

    // Date
    TYPE_CONVERTERS.put(
        DataTypes.DateType,
        (row, i, type) -> {
          if (row.isNullAt(i)) return Value.date(null);
          int days = row.getInt(i);
          java.time.LocalDate localDate = java.time.LocalDate.ofEpochDay(days);
          return Value.date(
              Date.fromYearMonthDay(
                  localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth()));
        });

    // Long array
    ARRAY_TYPE_CONVERTERS.put(
        DataTypes.LongType,
        (row, i, type) ->
            row.isNullAt(i)
                ? Value.int64Array((long[]) null)
                : Value.int64Array(row.getArray(i).toLongArray()));

    // String array
    ARRAY_TYPE_CONVERTERS.put(
        DataTypes.StringType,
        (row, i, type) -> {
          if (row.isNullAt(i)) return Value.stringArray(null);

          final DataType elementType = ((ArrayType) type).elementType();
          final Object[] items = (Object[]) row.getArray(i).toObjectArray(elementType);
          return Value.stringArray(
              java.util.Arrays.stream(items)
                  .map(item -> item == null ? null : item.toString())
                  .collect(java.util.stream.Collectors.toList()));
        });

    // Boolean
    ARRAY_TYPE_CONVERTERS.put(
        DataTypes.BooleanType,
        (row, i, type) ->
            row.isNullAt(i)
                ? Value.boolArray((boolean[]) null)
                : Value.boolArray(row.getArray(i).toBooleanArray()));

    // Double
    ARRAY_TYPE_CONVERTERS.put(
        DataTypes.DoubleType,
        (row, i, type) ->
            row.isNullAt(i)
                ? Value.float64Array((double[]) null)
                : Value.float64Array(row.getArray(i).toDoubleArray()));

    // Binary
    ARRAY_TYPE_CONVERTERS.put(
        DataTypes.BinaryType,
        (row, i, type) -> {
          if (row.isNullAt(i)) return Value.bytesArray(null);
          DataType elementType = ((ArrayType) type).elementType();
          Object[] items = (Object[]) row.getArray(i).toObjectArray(elementType);
          return Value.bytesArray(
              java.util.Arrays.stream(items)
                  .map(item -> item == null ? null : ByteArray.copyFrom((byte[]) item))
                  .collect(java.util.stream.Collectors.toList()));
        });

    // Timestamp
    ARRAY_TYPE_CONVERTERS.put(
        DataTypes.TimestampType,
        (row, i, type) -> {
          if (row.isNullAt(i)) return Value.timestampArray(null);
          long[] micros = row.getArray(i).toLongArray();
          if (micros == null) return Value.timestampArray(Collections.emptyList());
          List<Timestamp> timestamps = new ArrayList<>(micros.length);
          for (long micro : micros) {
            timestamps.add(Timestamp.ofTimeMicroseconds(micro));
          }
          return Value.timestampArray(timestamps);
        });

    // Date
    ARRAY_TYPE_CONVERTERS.put(
        DataTypes.DateType,
        (row, i, type) -> {
          if (row.isNullAt(i)) return Value.dateArray(null);
          int[] days = row.getArray(i).toIntArray();
          if (days == null) return Value.dateArray(Collections.emptyList());
          List<Date> dates = new ArrayList<>(days.length);

          for (int day : days) {
            java.time.LocalDate localDate = java.time.LocalDate.ofEpochDay(day);
            dates.add(
                Date.fromYearMonthDay(
                    localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth()));
          }
          return Value.dateArray(dates);
        });

    // Note: DecimalType is handled dynamically in the method below
    // because it relies on instanceof checks rather than strict equality.
  }

  public static Mutation internalRowToMutation(
      String tableName, InternalRow record, StructType schema) {

    Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(tableName);

    for (int i = 0; i < schema.length(); i++) {
      StructField field = schema.fields()[i];
      DataType fieldType = field.dataType();
      String fieldName = field.name();

      Value spannerValue = convertField(record, i, fieldType);
      builder.set(fieldName).to(spannerValue);
    }

    return builder.build();
  }

  // Helper method to resolve the strategy
  private static Value convertField(InternalRow row, int index, DataType type) {
    // Check exact matches
    FieldConverter converter = TYPE_CONVERTERS.get(type);
    if (converter != null) {
      return converter.convert(row, index, type);
    }

    // Handle dynamic types (Decimal, Array, Struct)
    if (type instanceof DecimalType) {
      if (row.isNullAt(index)) {
        return Value.numeric(null);
      }
      DecimalType dt = (DecimalType) type;
      BigDecimal bd = row.getDecimal(index, dt.precision(), dt.scale()).toJavaBigDecimal();
      return Value.numeric(bd);
    }

    if (type instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) type;
      DataType elementType = arrayType.elementType();
      FieldConverter arrayConverter = ARRAY_TYPE_CONVERTERS.get(elementType);
      if (arrayConverter != null) {
        return arrayConverter.convert(row, index, type);
      }

      if (elementType instanceof DecimalType) {
        if (row.isNullAt(index)) {
          return Value.numericArray(null);
        }
        ArrayData ad = row.getArray(index);
        BigDecimal[] a = (BigDecimal[]) ad.toObjectArray(elementType);
        return Value.numericArray(Arrays.asList(a));
      }
    }
    // TODO handle Struct here.
    // unsupported type
    throw new UnsupportedOperationException("Unsupported Spark DataType: " + type);
  }
}
