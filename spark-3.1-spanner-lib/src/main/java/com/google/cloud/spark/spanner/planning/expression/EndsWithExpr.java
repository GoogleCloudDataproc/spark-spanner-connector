package com.google.cloud.spark.spanner.planning.expression;

public final class EndsWithExpr implements BoolExpr {
  private final ColumnExpr left;
  private final LiteralExpr suffix;

  public EndsWithExpr(ColumnExpr left, LiteralExpr suffix) {
    this.left = left;
    this.suffix = suffix;
  }

  public ColumnExpr getLeft() {
    return left;
  }

  public LiteralExpr getSuffix() {
    return suffix;
  }

  @Override
  public <T> T accept(SpannerExprVisitor<T> visitor) {
    return visitor.visit(this);
  }
}
