package com.google.cloud.spark.spanner.rendering;

import java.util.Map;

public final class RenderResult {

  private final String sql;
  private final Map<String, Object> bindings;

  public RenderResult(String sql, Map<String, Object> bindings) {
    this.sql = sql;
    this.bindings = bindings;
  }

  public String getSql() {
    return sql;
  }

  public Map<String, Object> getBindings() {
    return bindings;
  }
}
