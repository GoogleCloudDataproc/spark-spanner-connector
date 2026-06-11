package com.google.cloud.spark.spanner.binding;

import com.google.cloud.spanner.Dialect;

public abstract class ParameterRegistry {
  protected int counter = 0;

  public abstract ParameterRef nextParameter();

  public static ParameterRegistry create(Dialect dialect) {
    switch (dialect) {
      case POSTGRESQL:
        return new PostgresSqlParameterRegistry();
      case GOOGLE_STANDARD_SQL:
        return new GoogleSqlParameterRegistry();
    }
    throw new IllegalArgumentException("Unsupported dialect: " + dialect);
  }
}
