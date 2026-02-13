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
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

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
          LocalDate localDate = LocalDate.ofEpochDay(days);
          return Value.date(
              Date.fromYearMonthDay(
                  localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth()));
        });

    // Long array
    ARRAY_TYPE_CONVERTERS.put(
        DataTypes.LongType,
        (row, i, type) -> {
          return arrayConverter(row, i, ArrayData::getLong, Value::int64Array);
        });

    // String array
    ARRAY_TYPE_CONVERTERS.put(
        DataTypes.StringType,
        (row, i, type) -> {
          return arrayConverter(
              row, i, (a, idx) -> a.getUTF8String(idx).toString(), Value::stringArray);
        });

    // Boolean
    ARRAY_TYPE_CONVERTERS.put(
        DataTypes.BooleanType,
        (row, i, type) -> {
          return arrayConverter(row, i, ArrayData::getBoolean, Value::boolArray);
        });

    // Double
    ARRAY_TYPE_CONVERTERS.put(
        DataTypes.DoubleType,
        (row, i, type) -> {
          return arrayConverter(row, i, ArrayData::getDouble, Value::float64Array);
        });

    // Binary
    ARRAY_TYPE_CONVERTERS.put(
        DataTypes.BinaryType,
        (row, i, type) -> {
          return arrayConverter(
              row, i, (a, idx) -> ByteArray.copyFrom(a.getBinary(idx)), Value::bytesArray);
        });

    // Timestamp
    ARRAY_TYPE_CONVERTERS.put(
        DataTypes.TimestampType,
        (row, i, type) -> {
          return arrayConverter(
              row,
              i,
              (a, idx) -> Timestamp.ofTimeMicroseconds(a.getLong(idx)),
              Value::timestampArray);
        });

    // Date
    ARRAY_TYPE_CONVERTERS.put(
        DataTypes.DateType,
        (row, i, type) -> {
          return arrayConverter(
              row,
              i,
              (a, idx) -> {
                LocalDate localDate = LocalDate.ofEpochDay(a.getInt(idx));
                return Date.fromYearMonthDay(
                    localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth());
              },
              Value::dateArray);
        });

    // Note: DecimalType is handled dynamically in the method below
    // because it relies on instanceof checks rather than strict equality.
  }

  // Allows Spark
  @FunctionalInterface
  private interface ArrayElementGetter<T> {
    T get(ArrayData array, int index);
  }

  private static <T> Value arrayConverter(
      InternalRow row,
      int i,
      ArrayElementGetter<T> elementGetter,
      Function<List<T>, Value> constructor) {

    if (row.isNullAt(i)) {
      return constructor.apply(null);
    }

    final ArrayData arrayData = row.getArray(i);
    final int numElements = arrayData.numElements();
    List<T> convertedList = new ArrayList<>(numElements);

    for (int j = 0; j < numElements; j++) {
      if (arrayData.isNullAt(j)) {
        convertedList.add(null);
      } else {
        // Uses specialized Spark getters (getLong, getDouble, etc.)
        convertedList.add(elementGetter.get(arrayData, j));
      }
    }

    return constructor.apply(convertedList);
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
        DecimalType dt = (DecimalType) elementType;
        return arrayConverter(
            row,
            index,
            (ad, i) -> ad.getDecimal(i, dt.precision(), dt.scale()).toJavaBigDecimal(),
            Value::numericArray);
      }
    }
    // TODO handle Struct here.
    // unsupported type
    throw new UnsupportedOperationException("Unsupported Spark DataType: " + type);
  }
}
