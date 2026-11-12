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
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Function;
import javax.annotation.Nullable;
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

  private static Mutation.WriteBuilder createBuilder(String tableName, Mutation.Op mode) {
    switch (Objects.requireNonNull(mode)) {
      case INSERT:
        return Mutation.newInsertBuilder(tableName);
      case UPDATE:
        return Mutation.newUpdateBuilder(tableName);
      case REPLACE:
        return Mutation.newReplaceBuilder(tableName);
      case DELETE:
        throw new UnsupportedOperationException("Unsupported Spanner write mode: DELETE");
      default:
        return Mutation.newInsertOrUpdateBuilder(tableName);
    }
  }

  public static Mutation internalRowToMutation(
      String tableName, InternalRow record, StructType schema, Map<String, String> spannerTypes) {
    return internalRowToMutation(
        tableName, record, schema, spannerTypes, Mutation.Op.INSERT_OR_UPDATE);
  }

  public static Mutation internalRowToMutation(
      String tableName,
      InternalRow record,
      StructType schema,
      Map<String, String> spannerTypes,
      Mutation.Op mode) {
    if (mode == Mutation.Op.DELETE) {
      List<String> extractedKeys = extractPrimaryKeysFromSchema(schema);
      if (extractedKeys.isEmpty()) {
        throw new UnsupportedOperationException(
            "Unsupported Spanner write mode: DELETE without primary key metadata");
      }
      return internalRowToDeleteMutation(tableName, record, schema, spannerTypes, extractedKeys);
    }

    Mutation.WriteBuilder builder = createBuilder(tableName, mode);

    for (int i = 0; i < schema.length(); i++) {
      StructField field = schema.fields()[i];
      DataType fieldType = field.dataType();
      String fieldName = field.name();
      String spannerType = lookupSpannerType(spannerTypes, fieldName);

      Value spannerValue = convertField(record, i, fieldType, spannerType);
      builder.set(fieldName).to(spannerValue);
    }

    return builder.build();
  }

  public static Mutation internalRowToMutation(
      String tableName,
      InternalRow record,
      StructType schema,
      Map<String, String> spannerTypes,
      Mutation.Op mode,
      List<String> primaryKeys) {
    if (mode == Mutation.Op.DELETE) {
      return internalRowToDeleteMutation(tableName, record, schema, spannerTypes, primaryKeys);
    }
    return internalRowToMutation(tableName, record, schema, spannerTypes, mode);
  }

  public static Mutation internalRowToDeleteMutation(
      String tableName,
      InternalRow record,
      StructType schema,
      Map<String, String> spannerTypes,
      List<String> primaryKeys) {
    Key key = internalRowToKey(record, schema, spannerTypes, primaryKeys);
    return Mutation.delete(tableName, key);
  }

  static Mutation internalRowToDeleteMutation(
      String tableName,
      InternalRow record,
      StructType schema,
      int[] pkIndices,
      String[] pkSpannerTypes) {
    Key key = internalRowToKey(record, schema, pkIndices, pkSpannerTypes);
    return Mutation.delete(tableName, key);
  }

  public static Key internalRowToKey(
      InternalRow record,
      StructType schema,
      Map<String, String> spannerTypes,
      List<String> primaryKeys) {
    int[] pkIndices = resolvePrimaryKeyIndices(schema, primaryKeys, null);
    String[] pkSpannerTypes = resolvePrimaryKeySpannerTypes(schema, spannerTypes, pkIndices);
    return internalRowToKey(record, schema, pkIndices, pkSpannerTypes);
  }

  static Key internalRowToKey(
      InternalRow record, StructType schema, int[] pkIndices, String[] pkSpannerTypes) {
    Key.Builder keyBuilder = Key.newBuilder();
    StructField[] fields = schema.fields();
    for (int i = 0; i < pkIndices.length; i++) {
      int fieldIndex = pkIndices[i];
      StructField field = fields[fieldIndex];
      appendKeyPart(keyBuilder, record, fieldIndex, field.dataType(), pkSpannerTypes[i]);
    }
    return keyBuilder.build();
  }

  static List<String> extractPrimaryKeysFromSchema(StructType schema) {
    if (schema == null) {
      return Collections.emptyList();
    }
    List<String> keys = new ArrayList<>();
    for (StructField field : schema.fields()) {
      if (field.metadata() != null
          && field.metadata().contains(SpannerUtils.PRIMARY_KEY_TAG)
          && field.metadata().getBoolean(SpannerUtils.PRIMARY_KEY_TAG)) {
        keys.add(field.name());
      }
    }
    return keys;
  }

  static int[] resolvePrimaryKeyIndices(
      StructType schema, List<String> primaryKeys, @Nullable String tableName) {
    String targetDesc = (tableName != null) ? " for Spanner table '" + tableName + "'" : "";
    if (primaryKeys == null || primaryKeys.isEmpty()) {
      throw new SpannerConnectorException(
          SpannerErrorCode.SCHEMA_VALIDATION_ERROR,
          "No primary key columns specified for DELETE" + targetDesc + ".");
    }

    Map<String, Integer> fieldIndexByName = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    StructField[] fields = schema.fields();
    for (int i = 0; i < fields.length; i++) {
      Integer existing = fieldIndexByName.put(fields[i].name(), i);
      if (existing != null) {
        for (String pk : primaryKeys) {
          if (pk.equalsIgnoreCase(fields[i].name())) {
            throw new SpannerConnectorException(
                SpannerErrorCode.SCHEMA_VALIDATION_ERROR,
                "Duplicate primary key column '"
                    + fields[i].name()
                    + "' in DataFrame"
                    + targetDesc
                    + ".");
          }
        }
      }
    }

    int[] indices = new int[primaryKeys.size()];
    List<String> missingKeys = new ArrayList<>();
    for (int i = 0; i < primaryKeys.size(); i++) {
      String pk = primaryKeys.get(i);
      Integer idx = fieldIndexByName.get(pk);
      if (idx == null) {
        missingKeys.add(pk);
      } else {
        indices[i] = idx;
      }
    }

    if (!missingKeys.isEmpty()) {
      throw new SpannerConnectorException(
          SpannerErrorCode.SCHEMA_VALIDATION_ERROR,
          "Missing primary key column(s) "
              + missingKeys
              + " in DataFrame for DELETE"
              + targetDesc
              + ". Required primary key columns: "
              + primaryKeys
              + ".");
    }
    return indices;
  }

  static String[] resolvePrimaryKeySpannerTypes(
      StructType schema, Map<String, String> spannerTypes, int[] pkIndices) {
    String[] resolved = new String[pkIndices.length];
    StructField[] fields = schema.fields();
    for (int i = 0; i < pkIndices.length; i++) {
      resolved[i] = lookupSpannerType(spannerTypes, fields[pkIndices[i]].name());
    }
    return resolved;
  }

  @Nullable
  private static String lookupSpannerType(
      @Nullable Map<String, String> spannerTypes, String fieldName) {
    if (spannerTypes == null) {
      return null;
    }
    String direct = spannerTypes.get(fieldName);
    if (direct != null) {
      return direct;
    }
    for (Map.Entry<String, String> entry : spannerTypes.entrySet()) {
      if (entry.getKey().equalsIgnoreCase(fieldName)) {
        return entry.getValue();
      }
    }
    return null;
  }

  private static void appendKeyPart(
      Key.Builder keyBuilder,
      InternalRow row,
      int index,
      DataType type,
      @Nullable String spannerType) {
    Value val = convertField(row, index, type, spannerType);
    if (val.isNull()) {
      keyBuilder.appendObject(null);
      return;
    }
    switch (val.getType().getCode()) {
      case BOOL:
        keyBuilder.append(val.getBool());
        break;
      case INT64:
        keyBuilder.append(val.getInt64());
        break;
      case FLOAT64:
        keyBuilder.append(val.getFloat64());
        break;
      case NUMERIC:
      case PG_NUMERIC:
        keyBuilder.append(val.getNumeric());
        break;
      case STRING:
        keyBuilder.append(val.getString());
        break;
      case BYTES:
        keyBuilder.append(val.getBytes());
        break;
      case TIMESTAMP:
        keyBuilder.append(val.getTimestamp());
        break;
      case DATE:
        keyBuilder.append(val.getDate());
        break;
      case UUID:
        keyBuilder.append(val.getUuid());
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported Spanner primary key type: " + val.getType());
    }
  }

  // Helper method to resolve the strategy
  private static Value convertField(InternalRow row, int index, DataType type, String spannerType) {
    // Destination-driven conversions first.
    if (type.equals(DataTypes.StringType) && isUuidType(spannerType)) {
      return convertUuid(row, index);
    }

    if (type instanceof ArrayType && isUuidArrayType(spannerType)) {
      ArrayType arrayType = (ArrayType) type;

      if (arrayType.elementType().equals(DataTypes.StringType)) {
        return convertUuidArray(row, index);
      }
    }

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

  private static Value convertUuid(InternalRow row, int index) {
    if (row.isNullAt(index)) {
      return Value.uuid(null);
    }

    String value = row.getUTF8String(index).toString();

    try {
      return Value.uuid(parseUuid(value));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid UUID value for Spanner UUID column: " + value, e);
    }
  }

  private static UUID parseUuid(String value) {
    try {
      UUID uuid = UUID.fromString(value);

      if (!uuid.toString().equalsIgnoreCase(value)) {
        throw new IllegalArgumentException("Not a canonical UUID");
      }

      return uuid;
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid UUID value: " + value, e);
    }
  }

  private static boolean isUuidType(String spannerType) {
    return spannerType != null && "UUID".equalsIgnoreCase(spannerType.trim());
  }

  private static boolean isUuidArrayType(String spannerType) {
    return spannerType != null
        && ("ARRAY<UUID>".equalsIgnoreCase(spannerType.trim())
            || "UUID[]".equalsIgnoreCase(spannerType.trim())
            || "UUID ARRAY".equalsIgnoreCase(spannerType.trim()));
  }

  private static Value convertUuidArray(InternalRow row, int index) {
    return arrayConverter(
        row,
        index,
        (array, idx) -> parseUuid(array.getUTF8String(idx).toString()),
        Value::uuidArray);
  }
}
