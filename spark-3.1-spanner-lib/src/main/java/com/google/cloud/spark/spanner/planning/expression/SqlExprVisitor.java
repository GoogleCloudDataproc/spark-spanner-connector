package com.google.cloud.spark.spanner.planning.expression;

import com.google.cloud.spark.spanner.binding.ParameterRegistry;
import com.google.cloud.spark.spanner.rendering.RenderResult;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class SqlExprVisitor implements SpannerExprVisitor<RenderResult> {

  private final ParameterRegistry parameterRegistry = new ParameterRegistry();

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

  @Override
  public RenderResult visit(IsNotNullExpr expr) {
    RenderResult value = expr.getValue().accept(this);

    return new RenderResult(value.getSql() + " IS NOT NULL", value.getBindings());
  }

  @Override
  public RenderResult visit(InExpr expr) {
    RenderResult left = expr.getLeft().accept(this);

    StringBuilder sql = new StringBuilder();
    sql.append(left.getSql());
    sql.append(" IN (");

    Map<String, Object> bindings = new LinkedHashMap<>();
    bindings.putAll(left.getBindings());
    List<LiteralExpr> values = expr.getValues();
    for (int i = 0; i < values.size(); i++) {
      RenderResult value = values.get(i).accept(this);

      if (i > 0) {
        sql.append(", ");
      }

      sql.append(value.getSql());
      bindings.putAll(value.getBindings());
    }

    sql.append(")");

    return new RenderResult(sql.toString(), bindings);
  }

  @Override
  public RenderResult visit(NotExpr expr) {
    RenderResult value = expr.getValue().accept(this);

    return new RenderResult("NOT(" + value.getSql() + ")", value.getBindings());
  }
}
