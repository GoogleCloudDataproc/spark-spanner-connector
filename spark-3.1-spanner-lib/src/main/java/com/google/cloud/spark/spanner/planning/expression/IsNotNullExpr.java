package com.google.cloud.spark.spanner.planning.expression;

public final class IsNotNullExpr implements BoolExpr {
  private final ValueExpr value;

  public IsNotNullExpr(ValueExpr value) {
    this.value = value;
  }

  public ValueExpr getValue() {
    return value;
  }

  @Override
  public <T> T accept(SpannerExprVisitor<T> visitor) {
    return visitor.visit(this);
  }
}
