package com.google.cloud.spark.spanner.planning.expression;

public interface SpannerExprVisitor<T> {
  T visit(EqExpr expr);

  T visit(AndExpr expr);

  T visit(OrExpr expr);

  T visit(ColumnExpr expr);

  T visit(LiteralExpr expr);

  T visit(TrueExpr expr);
}
