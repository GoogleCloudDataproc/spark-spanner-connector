package com.google.cloud.spark.spanner.binding;

import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Type;
import com.google.cloud.spark.spanner.planning.LiteralExpr;
import java.math.BigDecimal;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;

public final class SpannerTypeBinder {

  public void bind(Statement.Builder builder, String parameter, LiteralExpr literal) {

    Object value = literal.getValue();
    DataType type = literal.getSparkType();

    if (value == null) {
      builder.bind(parameter).to(mapToSpannerType(type), (com.google.cloud.spanner.Struct) null);
      return;
    }

    if (type instanceof DecimalType) {
      BigDecimal bd = (BigDecimal) value;
      // Spanner NUMERIC validation: precision <= 38, scale <= 9
      if (bd.precision() <= 38 && bd.scale() <= 9) {
        builder.bind(parameter).to(bd);
      } else {
        throw new IllegalArgumentException("Decimal out of Spanner NUMERIC range");
      }
    } else if (type.sameType(DataTypes.StringType)) {
      builder.bind(parameter).to((String) value);
    } else if (type.sameType(DataTypes.LongType)) {
      builder.bind(parameter).to((Long) value);
    } else if (type.sameType(DataTypes.BooleanType)) {
      builder.bind(parameter).to((Boolean) value);
    } else {
      throw new UnsupportedOperationException("Unsupported type: " + type);
    }
  }

  private Type mapToSpannerType(DataType type) {
    if (type instanceof DecimalType) {
      return Type.numeric();
    } else if (type.sameType(DataTypes.StringType)) {
      return Type.string();
    } else if (type.sameType(DataTypes.LongType)) {
      return Type.int64();
    } else if (type.sameType(DataTypes.BooleanType)) {
      return Type.bool();
    } else {
      throw new UnsupportedOperationException(
          "Unsupported Spark type for Spanner null mapping: " + type);
    }
  }
}
