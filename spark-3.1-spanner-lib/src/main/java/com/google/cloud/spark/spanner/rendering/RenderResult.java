package com.google.cloud.spark.spanner.rendering;

import com.google.cloud.spark.spanner.planning.expression.LiteralExpr;
import java.util.Map;

public final class RenderResult {

  private final String sql;
  private final Map<String, LiteralExpr> bindings;

  public RenderResult(String sql, Map<String, LiteralExpr> bindings) {
    this.sql = sql;
    this.bindings = bindings;
  }

  public String getSql() {
    return sql;
  }

  public Map<String, LiteralExpr> getBindings() {
    return bindings;
  }
}
