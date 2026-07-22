// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.google.cloud.spark.spanner.rendering;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spark.spanner.SpannerInformationSchema;
import com.google.cloud.spark.spanner.binding.ParameterRegistry;
import com.google.cloud.spark.spanner.planning.expression.*;
import java.util.*;

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

    Map<String, LiteralExpr> bindings = new LinkedHashMap<>(left.getBindings());

    Iterator<ValueExpr> iterator = expr.getValues().iterator();
    while (iterator.hasNext()) {
      RenderResult value = iterator.next().accept(this);

      sql.append(value.getSql());
      bindings.putAll(value.getBindings());

      if (iterator.hasNext()) {
        sql.append(", ");
      }
    }

    sql.append(')');

    return new RenderResult(sql.toString(), bindings);
  }

  @Override
  public RenderResult visit(NotExpr expr) {
    RenderResult value = expr.getValue().accept(this);

    return new RenderResult("NOT " + parenthesize(value.getSql()), value.getBindings());
  }

  abstract String renderStartsWith(String left, String right);

  abstract String renderEndsWith(String left, String right);

  abstract String renderContains(String left, String right);

  @Override
  public RenderResult visit(StartsWithExpr expr) {

    RenderResult left = expr.getLeft().accept(this);
    RenderResult value = expr.getPrefix().accept(this);

    Map<String, LiteralExpr> bindings = new LinkedHashMap<>();
    bindings.putAll(left.getBindings());
    bindings.putAll(value.getBindings());

    return new RenderResult(renderStartsWith(left.getSql(), value.getSql()), bindings);
  }

  @Override
  public RenderResult visit(EndsWithExpr expr) {

    RenderResult left = expr.getLeft().accept(this);
    RenderResult value = expr.getSuffix().accept(this);

    Map<String, LiteralExpr> bindings = new LinkedHashMap<>();
    bindings.putAll(left.getBindings());
    bindings.putAll(value.getBindings());

    return new RenderResult(renderEndsWith(left.getSql(), value.getSql()), bindings);
  }

  @Override
  public RenderResult visit(ContainsExpr expr) {

    RenderResult left = expr.getLeft().accept(this);
    RenderResult value = expr.getValue().accept(this);

    Map<String, LiteralExpr> bindings = new LinkedHashMap<>();
    bindings.putAll(left.getBindings());
    bindings.putAll(value.getBindings());

    return new RenderResult(renderContains(left.getSql(), value.getSql()), bindings);
  }

  @Override
  public RenderResult visit(ArithmeticExpr expr) {
    RenderResult left = expr.getLeft().accept(this);
    RenderResult right = expr.getRight().accept(this);
    switch (expr.getOperator()) {
      case ADD:
        return new RenderResult(
            left.getSql() + "+" + right.getSql(), merge(left.getBindings(), right.getBindings()));

      case SUBTRACT:
        return new RenderResult(
            left.getSql() + "-" + right.getSql(), merge(left.getBindings(), right.getBindings()));

      case MULTIPLY:
        return new RenderResult(
            left.getSql() + "*" + right.getSql(), merge(left.getBindings(), right.getBindings()));

      case DIVIDE:
        return new RenderResult(
            left.getSql() + "/" + right.getSql(), merge(left.getBindings(), right.getBindings()));

      case MOD:
        return new RenderResult(
            "MOD(" + left.getSql() + "," + right.getSql() + ")",
            merge(left.getBindings(), right.getBindings()));

      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported operator: %s", expr.getOperator()));
    }
  }

  @Override
  public RenderResult visit(UnaryExpr expr) {
    RenderResult operand = expr.getOperand().accept(this);

    switch (expr.getOperator()) {
      case PLUS:
        return new RenderResult("+(" + operand.getSql() + ")", operand.getBindings());

      case NEGATE:
        return new RenderResult("-(" + operand.getSql() + ")", operand.getBindings());

      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported operator: %s", expr.getOperator()));
    }
  }

  @Override
  public RenderResult visit(FunctionExpr expr) {
    return renderFunction(expr.function(), expr.arguments());
  }

  private String renderFunctionName(FunctionExpr.Function function) {

    switch (function) {
      case LOWER:
        return "LOWER";

      case UPPER:
        return "UPPER";

      case LENGTH:
        return "LENGTH";

      case CONCAT:
        return "CONCAT";

      case COALESCE:
        return "COALESCE";

      case GREATEST:
        return "GREATEST";

      case LEAST:
        return "LEAST";

      default:
        throw new UnsupportedOperationException("Unsupported function: " + function);
    }
  }

  private RenderResult renderFunction(FunctionExpr.Function function, List<ValueExpr> arguments) {

    StringBuilder sql = new StringBuilder();

    sql.append(renderFunctionName(function));
    sql.append("(");

    Map<String, LiteralExpr> bindings = new LinkedHashMap<>();

    for (int i = 0; i < arguments.size(); i++) {

      RenderResult result = arguments.get(i).accept(this);

      if (i > 0) {
        sql.append(", ");
      }

      sql.append(result.getSql());
      bindings.putAll(result.getBindings());
    }

    sql.append(')');

    return new RenderResult(sql.toString(), bindings);
  }

  private String parenthesize(String in) {
    return "(" + in + ")";
  }
}
