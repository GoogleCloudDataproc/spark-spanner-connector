package com.google.cloud.spark.spanner.binding;

public class PostgresSqlParameterRegistry extends ParameterRegistry {
  @Override
  public ParameterRef nextParameter() {
    counter++;
    final String name = String.valueOf(counter);
    return new ParameterRef("p" + name, name);
  }
}
