package com.google.cloud.spark.spanner.planning.expression;

public final class NotExpr implements BoolExpr {
  private final BoolExpr value;

  public NotExpr(BoolExpr value) {
    this.value = value;
  }

  public BoolExpr getValue() {
    return value;
  }

  @Override
  public <T> T accept(SpannerExprVisitor<T> visitor) {
    return visitor.visit(this);
  }
}
