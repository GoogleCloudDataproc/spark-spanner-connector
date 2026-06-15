package com.google.cloud.spark.spanner.binding;

public class ParameterRef {
  private final String bindName;
  private final String sqlName;

  public ParameterRef(String bindName, String sqlName) {
    this.bindName = bindName;
    this.sqlName = sqlName;
  }

  public String getBindName() {
    return bindName;
  }

  public String getSqlName() {
    return sqlName;
  }
}
