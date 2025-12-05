package com.google.cloud.spark.spanner;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import java.math.BigDecimal;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SpannerWriterUtils {

  public static Mutation internalRowToMutation(
      String tableName, InternalRow record, StructType schema) {
    Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(tableName);
    for (int i = 0; i < schema.length(); i++) {
      StructField field = schema.fields()[i];
      String fieldName = field.name();
      DataType fieldType = field.dataType();

      if (record.isNullAt(i)) {
        // TODO: Handle null values correctly.
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
      } else if (fieldType.sql().startsWith("DECIMAL")) {
        // TODO Ensure we do not lose precision here
        BigDecimal bd = record.getDecimal(i, 38, 9).toJavaBigDecimal();
        builder.set(fieldName).to(bd);
      }
      // TODO: Add support for ArrayType and StructType.
    }
    return builder.build();
  }
}
