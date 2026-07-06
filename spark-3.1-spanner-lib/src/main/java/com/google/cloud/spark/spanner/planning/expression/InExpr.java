package com.google.cloud.spark.spanner.planning.expression;

import java.util.List;

public final class InExpr implements BoolExpr {
  private final ColumnExpr left;
  private final List<LiteralExpr> values;

  public InExpr(ColumnExpr left, List<LiteralExpr> values) {
    this.left = left;
    this.values = values;
  }

  public ColumnExpr getLeft() {
    return left;
  }

  public List<LiteralExpr> getValues() {
    return values;
  }

  @Override
  public <T> T accept(SpannerExprVisitor<T> visitor) {
    return visitor.visit(this);
  }
}
