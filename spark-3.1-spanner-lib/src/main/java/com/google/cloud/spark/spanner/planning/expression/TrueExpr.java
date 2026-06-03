package com.google.cloud.spark.spanner.planning.expression;

public final class TrueExpr implements BoolExpr {
  @Override
  public <T> T accept(SpannerExprVisitor<T> visitor) {
    return visitor.visit(this);
  }
}
