package com.google.cloud.spark.spanner.rendering;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spark.spanner.SpannerInformationSchema;
import com.google.cloud.spark.spanner.binding.ParameterRegistry;
import com.google.cloud.spark.spanner.planning.expression.*;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class SqlExprVisitor implements SpannerExprVisitor<RenderResult> {

  private final ParameterRegistry parameterRegistry = new ParameterRegistry();
  private SpannerInformationSchema infoSchema;
  private Dialect dialect;

  public SqlExprVisitor(Dialect dialect) {
    infoSchema = SpannerInformationSchema.create(dialect);
    this.dialect = dialect;
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
  public RenderResult visit(LiteralExpr expr) {
    String parameter = parameterRegistry.nextParameter();

    if (dialect == Dialect.GOOGLE_STANDARD_SQL) {
      return new RenderResult("@p" + parameter, Collections.singletonMap("p" + parameter, expr));
    } else {
      return new RenderResult("$" + parameter, Collections.singletonMap("p" + parameter, expr));
    }
  }

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

    return new RenderResult("NOT" + parenthesize(value.getSql()), value.getBindings());
  }

  private RenderResult like(ColumnExpr column, LiteralExpr pattern) {

    RenderResult left = column.accept(this);

    String parameter = parameterRegistry.nextParameter();

    if (dialect == Dialect.GOOGLE_STANDARD_SQL) {
      return new RenderResult(
          left.getSql() + " LIKE @p" + parameter,
          Collections.singletonMap("p" + parameter, pattern));
    } else {
      return new RenderResult(
          left.getSql() + " LIKE $" + parameter,
          Collections.singletonMap("p" + parameter, pattern));
    }
  }

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
