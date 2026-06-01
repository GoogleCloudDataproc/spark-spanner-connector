package com.google.cloud.spark.spanner.planning;

import org.apache.spark.sql.types.DataType;

public final class LiteralExpr implements ValueExpr {

  private final Object value;
  private final DataType sparkType;

  public LiteralExpr(Object value, DataType sparkType) {
    this.value = value;
    this.sparkType = sparkType;
  }

  public Object getValue() {
    return value;
  }

  public DataType getSparkType() {
    return sparkType;
  }

  public <T> T accept(SpannerExprVisitor<T> visitor) {
    return visitor.visit(this);
  }
}
