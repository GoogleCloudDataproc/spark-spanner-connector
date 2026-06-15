package com.google.cloud.spark.spanner.planning.expression;

public interface SpannerExprVisitor<T> {
  T visit(EqExpr expr);

  T visit(GtExpr expr);

  T visit(GteExpr expr);

  T visit(LtExpr expr);

  T visit(LteExpr expr);

  T visit(AndExpr expr);

  T visit(OrExpr expr);

  T visit(ColumnExpr expr);

  T visit(LiteralExpr expr);

  T visit(TrueExpr expr);

  T visit(IsNullExpr expr);

  T visit(IsNotNullExpr expr);

  T visit(InExpr expr);

  T visit(NotExpr expr);

  T visit(StartsWithExpr expr);

  T visit(EndsWithExpr expr);

  T visit(ContainsExpr expr);
}
