package com.google.cloud.spark.spanner.planning.expression;

public final class AndExpr implements BoolExpr {
  private final BoolExpr left;
  private final BoolExpr right;

  public AndExpr(BoolExpr left, BoolExpr right) {
    this.left = left;
    this.right = right;
  }

  public BoolExpr getLeft() {
    return left;
  }

  public BoolExpr getRight() {
    return right;
  }

  @Override
  public <T> T accept(SpannerExprVisitor<T> visitor) {
    return visitor.visit(this);
  }
}
