package com.google.cloud.spark.spanner.planning.expression;

import org.apache.spark.sql.types.DataType;

public final class ColumnExpr implements ValueExpr {
  private final String columnName;
  private final DataType sparkType;

  public boolean isNullable() {
    return nullable;
  }

  public DataType getSparkType() {
    return sparkType;
  }

  private final boolean nullable;

  public ColumnExpr(String columnName, DataType sparkType, boolean nullable) {
    this.columnName = columnName;
    this.sparkType = sparkType;
    this.nullable = nullable;
  }

  public String getColumnName() {
    return columnName;
  }

  @Override
  public <T> T accept(SpannerExprVisitor<T> visitor) {
    return visitor.visit(this);
  }
}
