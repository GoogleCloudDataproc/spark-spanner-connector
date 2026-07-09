package com.google.cloud.spark.spanner.rendering;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spark.spanner.SpannerInformationSchema;
import com.google.cloud.spark.spanner.binding.ParameterRegistry;
import com.google.cloud.spark.spanner.planning.expression.*;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public abstract class SqlExprVisitor implements SpannerExprVisitor<RenderResult> {

  protected ParameterRegistry parameterRegistry = null;
  private SpannerInformationSchema infoSchema = null;

  protected SqlExprVisitor(
      SpannerInformationSchema infoSchema, ParameterRegistry parameterRegistry) {
    this.infoSchema = infoSchema;
    this.parameterRegistry = parameterRegistry;
  }

  public static SqlExprVisitor create(Dialect dialect) {
    switch (dialect) {
      case POSTGRESQL:
        return new PostgresSpannerSqlExprVisitor();
      case GOOGLE_STANDARD_SQL:
        return new GoogleSqlSpannerSqlExprVisitor();
    }
    throw new IllegalArgumentException("Unsupported dialect: " + dialect);
  }

  private static Map<String, LiteralExpr> merge(
      Map<String, LiteralExpr> left, Map<String, LiteralExpr> right) {

    Map<String, LiteralExpr> result = new LinkedHashMap<>();
    result.putAll(left);
    result.putAll(right);
    return result;
  }

  @Override
  public RenderResult visit(ColumnExpr expr) {
    return new RenderResult(
        infoSchema.quoteIdentifier(expr.getColumnName()), Collections.emptyMap());
  }

  @Override
  public RenderResult visit(EqExpr expr) {
    RenderResult left = expr.getLeft().accept(this);

    RenderResult right = expr.getRight().accept(this);

    return new RenderResult(
        left.getSql() + " = " + right.getSql(), merge(left.getBindings(), right.getBindings()));
  }

  @Override
  public RenderResult visit(EqNullSafeExpr expr) {
    RenderResult left = expr.getLeft().accept(this);

    RenderResult right = expr.getRight().accept(this);

    return new RenderResult(
        left.getSql() + " IS NOT DISTINCT FROM " + right.getSql(),
        merge(left.getBindings(), right.getBindings()));
  }

  @Override
  public RenderResult visit(GtExpr expr) {
    RenderResult left = expr.getLeft().accept(this);

    RenderResult right = expr.getRight().accept(this);

    return new RenderResult(
        left.getSql() + " > " + right.getSql(), merge(left.getBindings(), right.getBindings()));
  }

  @Override
  public RenderResult visit(GteExpr expr) {
    RenderResult left = expr.getLeft().accept(this);

    RenderResult right = expr.getRight().accept(this);

    return new RenderResult(
        left.getSql() + " >= " + right.getSql(), merge(left.getBindings(), right.getBindings()));
  }

  @Override
  public RenderResult visit(LtExpr expr) {
    RenderResult left = expr.getLeft().accept(this);

    RenderResult right = expr.getRight().accept(this);

    return new RenderResult(
        left.getSql() + " < " + right.getSql(), merge(left.getBindings(), right.getBindings()));
  }

  @Override
  public RenderResult visit(LteExpr expr) {
    RenderResult left = expr.getLeft().accept(this);

    RenderResult right = expr.getRight().accept(this);

    return new RenderResult(
        left.getSql() + " <= " + right.getSql(), merge(left.getBindings(), right.getBindings()));
  }

  @Override
  public RenderResult visit(AndExpr expr) {
    RenderResult left = expr.getLeft().accept(this);

    RenderResult right = expr.getRight().accept(this);

    return new RenderResult(
        left.getSql() + " AND " + right.getSql(), merge(left.getBindings(), right.getBindings()));
  }

  @Override
  public RenderResult visit(OrExpr expr) {
    RenderResult left = expr.getLeft().accept(this);

    RenderResult right = expr.getRight().accept(this);

    return new RenderResult(
        left.getSql() + " OR " + right.getSql(), merge(left.getBindings(), right.getBindings()));
  }

  @Override
  public abstract RenderResult visit(LiteralExpr expr);

  @Override
  public RenderResult visit(TrueExpr expr) {
    return new RenderResult("TRUE", Collections.emptyMap());
  }

  @Override
  public RenderResult visit(IsNullExpr expr) {
    RenderResult value = expr.getValue().accept(this);

    return new RenderResult(value.getSql() + " IS NULL", value.getBindings());
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

    Map<String, LiteralExpr> bindings = new LinkedHashMap<>();
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

    return new RenderResult("NOT " + parenthesize(value.getSql()), value.getBindings());
  }

  abstract RenderResult like(ColumnExpr column, LiteralExpr pattern);

  @Override
  public RenderResult visit(StartsWithExpr expr) {
    return like(
        expr.getLeft(),
        new LiteralExpr(expr.getPrefix().getValue() + "%", expr.getPrefix().getSparkType()));
  }

  @Override
  public RenderResult visit(EndsWithExpr expr) {
    return like(
        expr.getLeft(),
        new LiteralExpr("%" + expr.getSuffix().getValue(), expr.getSuffix().getSparkType()));
  }

  @Override
  public RenderResult visit(ContainsExpr expr) {
    return like(
        expr.getLeft(),
        new LiteralExpr("%" + expr.getValue().getValue() + "%", expr.getValue().getSparkType()));
  }

  private String parenthesize(String in) {
    return "(" + in + ")";
  }
}
