package com.google.cloud.spark.spanner.planning.expression;

public final class GteExpr implements BoolExpr {
  private final ValueExpr left;
  private final ValueExpr right;

  public GteExpr(ValueExpr left, ValueExpr right) {
    this.left = left;
    this.right = right;
  }

  public ValueExpr getLeft() {
    return left;
  }

  public ValueExpr getRight() {
    return right;
  }

  @Override
  public <T> T accept(SpannerExprVisitor<T> visitor) {
    return visitor.visit(this);
  }
}
