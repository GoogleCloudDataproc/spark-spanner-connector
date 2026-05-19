package com.google.cloud.spark.spanner.planning;

public final class TableRelation implements Relation {
  private final String tableName;
  private final String alias;

  public TableRelation(String tableName, String alias) {
    this.tableName = tableName;
    this.alias = alias;
  }
}