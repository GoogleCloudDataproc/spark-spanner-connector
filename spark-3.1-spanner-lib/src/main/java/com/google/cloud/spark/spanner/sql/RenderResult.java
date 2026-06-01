package com.google.cloud.spark.spanner.sql;

import java.util.Map;

public final class RenderResult {
  private final String sql;
  private final Map<String, String>
      parameters; // parameter name, bound value (TODO: does this need to support multiple types?)

  public RenderResult(String sql, Map<String, String> parameters) {
    this.sql = sql;
    this.parameters = parameters;
  }
}
