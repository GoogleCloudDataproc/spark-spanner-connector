package com.google.cloud.spark.spanner.binding;

public class PostgresSqlParameterRegistry extends ParameterRegistry {
  @Override
  public ParameterRef nextParameter() {
    counter++;
    final String name = String.valueOf(counter);
    ParameterRef ref = new ParameterRef("p" + String.valueOf(counter), String.valueOf(counter));
    return ref;
  }
}
