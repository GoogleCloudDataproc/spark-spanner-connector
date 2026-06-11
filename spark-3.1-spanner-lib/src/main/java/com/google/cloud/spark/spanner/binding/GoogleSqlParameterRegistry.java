package com.google.cloud.spark.spanner.binding;

public class GoogleSqlParameterRegistry extends ParameterRegistry {
  @Override
  public ParameterRef nextParameter() {
    counter++;
    final String name = "p" + String.valueOf(counter);
    ParameterRef ref = new ParameterRef(name, name);
    return ref;
  }
}
