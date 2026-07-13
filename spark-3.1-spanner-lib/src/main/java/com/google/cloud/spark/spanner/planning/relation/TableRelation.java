package com.google.cloud.spark.spanner.planning.relation;

import com.google.cloud.spark.spanner.scan.SpannerTable;

public final class TableRelation implements Relation {
  private final String tableName;

  public String getAlias() {
    return alias;
  }

  public String getTableName() {
    return tableName;
  }

  public SpannerTable getTable() {
    return table;
  }

  private final String alias;

  private final SpannerTable table;

  public TableRelation(String tableName, String alias, SpannerTable table) {
    this.tableName = tableName;
    this.alias = alias;
    this.table = table;
  }

  @Override
  public <T> T accept(RelationVisitor<T> visitor) {
    return visitor.visit(this);
  }
}
