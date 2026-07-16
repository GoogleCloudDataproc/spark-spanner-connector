package com.google.cloud.spark.spanner.rendering;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spark.spanner.SpannerInformationSchema;
import com.google.cloud.spark.spanner.binding.ParameterRef;
import com.google.cloud.spark.spanner.binding.ParameterRegistry;
import com.google.cloud.spark.spanner.planning.expression.LiteralExpr;
import java.util.Collections;

public class PostgresSpannerSqlExprVisitor extends SqlExprVisitor {

  public PostgresSpannerSqlExprVisitor() {
    super(
        SpannerInformationSchema.create(Dialect.POSTGRESQL),
        ParameterRegistry.create(Dialect.POSTGRESQL));
  }

  @Override
  public RenderResult visit(LiteralExpr expr) {
    ParameterRef ref = parameterRegistry.nextParameter();

    return new RenderResult(
        "$" + ref.getSqlName(), Collections.singletonMap(ref.getBindName(), expr));
  }

  @Override
  public String renderStartsWith(String left, String right) {
    return left + " LIKE '" + right + "%'";
  }

  @Override
  public String renderEndsWith(String left, String right) {
    return left + " LIKE '%" + right + "'";
  }

  @Override
  public String renderContains(String left, String right) {
    return left + " LIKE '%" + right + "%'";
  }
}
