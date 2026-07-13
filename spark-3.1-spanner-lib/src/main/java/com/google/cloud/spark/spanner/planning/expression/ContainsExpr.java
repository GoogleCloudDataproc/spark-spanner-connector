package com.google.cloud.spark.spanner.planning.expression;

public final class ContainsExpr implements BoolExpr {
  private final ColumnExpr left;
  private final LiteralExpr value;

  public ContainsExpr(ColumnExpr left, LiteralExpr value) {
    this.left = left;
    this.value = value;
  }

  public ColumnExpr getLeft() {
    return left;
  }

  public LiteralExpr getValue() {
    return value;
  }

  @Override
  public <T> T accept(SpannerExprVisitor<T> visitor) {
    return visitor.visit(this);
  }
}
