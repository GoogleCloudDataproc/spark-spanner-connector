package com.google.cloud.spark.spanner;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import java.math.BigDecimal;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.*;

public class SpannerWriterUtils {

  public static Mutation internalRowToMutation(
      String tableName, InternalRow record, StructType schema) {
    Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(tableName);
    for (int i = 0; i < schema.length(); i++) {
      StructField field = schema.fields()[i];
      String fieldName = field.name();
      DataType fieldType = field.dataType();
      if (record.isNullAt(i)) {
        if (fieldType.equals(DataTypes.LongType)) {
          builder.set(fieldName).to(Value.int64(null));
        } else if (fieldType.equals(DataTypes.StringType)) {
          builder.set(fieldName).to(Value.string(null));
        } else if (fieldType.equals(DataTypes.BooleanType)) {
          builder.set(fieldName).to(Value.bool(null));
        } else if (fieldType.equals(DataTypes.DoubleType)) {
          builder.set(fieldName).to(Value.float64(null));
        } else if (fieldType.equals(DataTypes.TimestampType)) {
          builder.set(fieldName).to(Value.timestamp(null));
        } else if (fieldType.equals(DataTypes.DateType)) {
          builder.set(fieldName).to(Value.date(null));
        } else if (fieldType.equals(DataTypes.BinaryType)) {
          builder.set(fieldName).to(Value.bytes(null));
        } else if (fieldType instanceof DecimalType) {
          builder.set(fieldName).to(Value.numeric(null));
        }
        continue;
      }

      if (fieldType.equals(DataTypes.LongType)) {
        builder.set(fieldName).to(record.getLong(i));
      } else if (fieldType.equals(DataTypes.StringType)) {
        builder.set(fieldName).to(record.getString(i));
      } else if (fieldType.equals(DataTypes.BooleanType)) {
        builder.set(fieldName).to(record.getBoolean(i));
      } else if (fieldType.equals(DataTypes.DoubleType)) {
        builder.set(fieldName).to(record.getDouble(i));
      } else if (fieldType.equals(DataTypes.TimestampType)) {
        long microseconds = record.getLong(i);
        builder.set(fieldName).to(Timestamp.ofTimeMicroseconds(microseconds));
      } else if (fieldType.equals(DataTypes.DateType)) {
        int days = record.getInt(i);
        java.time.LocalDate localDate = java.time.LocalDate.ofEpochDay(days);
        builder
            .set(fieldName)
            .to(
                Date.fromYearMonthDay(
                    localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth()));
      } else if (fieldType.equals(DataTypes.BinaryType)) {
        builder.set(fieldName).to(ByteArray.copyFrom(record.getBinary(i)));
      } else if (fieldType instanceof org.apache.spark.sql.types.DecimalType) {
        // TODO Ensure we do not lose precision here
        org.apache.spark.sql.types.DecimalType dt =
            (org.apache.spark.sql.types.DecimalType) fieldType;
        BigDecimal bd = record.getDecimal(i, dt.precision(), dt.scale()).toJavaBigDecimal();
        builder.set(fieldName).to(bd);
      }
      // TODO: Add support for ArrayType and StructType.
    }
    return builder.build();
  }
}
