package com.google.cloud.spark.spanner.planning.relation;

public final class TableRelation implements Relation {
  private final String tableName;

  public String getAlias() {
    return alias;
  }

  public String getTableName() {
    return tableName;
  }

  private final String alias;

  public TableRelation(String tableName, String alias) {
    this.tableName = tableName;
    this.alias = alias;
  }

  @Override
  public <T> T accept(RelationVisitor<T> visitor) {
    return visitor.visit(this);
  }
}
