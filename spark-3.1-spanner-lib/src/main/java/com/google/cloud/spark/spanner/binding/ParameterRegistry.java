package com.google.cloud.spark.spanner.binding;

public final class ParameterRegistry {
  private int counter = 0;

  public String nextParameter() {
    counter++;
    //    return "p" + counter;
    return String.valueOf(counter);
  }
}
