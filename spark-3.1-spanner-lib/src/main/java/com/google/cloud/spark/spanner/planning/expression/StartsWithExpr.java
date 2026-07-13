package com.google.cloud.spark.spanner.planning.expression;

public final class StartsWithExpr implements BoolExpr {
  private final ColumnExpr left;
  private final LiteralExpr prefix;

  public StartsWithExpr(ColumnExpr left, LiteralExpr prefix) {
    this.left = left;
    this.prefix = prefix;
  }

  public ColumnExpr getLeft() {
    return left;
  }

  public LiteralExpr getPrefix() {
    return prefix;
  }

  @Override
  public <T> T accept(SpannerExprVisitor<T> visitor) {
    return visitor.visit(this);
  }
}
