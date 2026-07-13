package com.google.cloud.spark.spanner.planning.expression;

public final class EqNullSafeExpr implements BoolExpr {
  private final ValueExpr left;
  private final ValueExpr right;

  public EqNullSafeExpr(ValueExpr left, ValueExpr right) {
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
