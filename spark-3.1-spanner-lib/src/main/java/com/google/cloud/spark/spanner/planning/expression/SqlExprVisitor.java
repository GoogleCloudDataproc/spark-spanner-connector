package com.google.cloud.spark.spanner.planning.expression;

import com.google.cloud.spark.spanner.binding.ParameterRegistry;
import com.google.cloud.spark.spanner.rendering.RenderResult;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public final class SqlExprVisitor implements SpannerExprVisitor<RenderResult> {

  private ParameterRegistry parameterRegistry = new ParameterRegistry();

  private static Map<String, Object> merge(Map<String, Object> left, Map<String, Object> right) {

    Map<String, Object> result = new LinkedHashMap<>();
    result.putAll(left);
    result.putAll(right);
    return result;
  }

  @Override
  public RenderResult visit(ColumnExpr expr) {
    return new RenderResult(expr.getColumnName(), Collections.emptyMap());
  }

  @Override
  public RenderResult visit(EqExpr expr) {
    RenderResult left = expr.getLeft().accept(this);

    RenderResult right = expr.getRight().accept(this);

    return new RenderResult(
        left.getSql() + " = " + right.getSql(), merge(left.getBindings(), right.getBindings()));
  }

  @Override
  public RenderResult visit(GtExpr expr) {
    RenderResult left = expr.getLeft().accept(this);

    RenderResult right = expr.getRight().accept(this);

    return new RenderResult(
        left.getSql() + " > " + right.getSql(), merge(left.getBindings(), right.getBindings()));
  }

  @Override
  public RenderResult visit(LtExpr expr) {
    RenderResult left = expr.getLeft().accept(this);

    RenderResult right = expr.getRight().accept(this);

    return new RenderResult(
        left.getSql() + " < " + right.getSql(), merge(left.getBindings(), right.getBindings()));
  }

  @Override
  public RenderResult visit(AndExpr expr) {
    RenderResult left = expr.getLeft().accept(this);

    RenderResult right = expr.getRight().accept(this);

    return new RenderResult(
        "(" + left.getSql() + " AND " + right.getSql() + ")",
        merge(left.getBindings(), right.getBindings()));
  }

  @Override
  public RenderResult visit(OrExpr expr) {
    RenderResult left = expr.getLeft().accept(this);

    RenderResult right = expr.getRight().accept(this);

    return new RenderResult(
        "(" + left.getSql() + " OR " + right.getSql() + ")",
        merge(left.getBindings(), right.getBindings()));
  }

  @Override
  public RenderResult visit(LiteralExpr expr) {
    String parameter = parameterRegistry.nextParameter();

    return new RenderResult("@" + parameter, Collections.singletonMap(parameter, expr.getValue()));
  }

  @Override
  public RenderResult visit(TrueExpr expr) {
    return new RenderResult("TRUE", Collections.emptyMap());
  }
}
