package com.google.cloud.spark.spanner.planning.expression;

public final class ColumnExpr implements ValueExpr {
  private final String columnName;

  public ColumnExpr(String columnName) {
    this.columnName = columnName;
  }

  public String getColumnName() {
    return columnName;
  }

  @Override
  public <T> T accept(SpannerExprVisitor<T> visitor) {
    return visitor.visit(this);
  }
}
