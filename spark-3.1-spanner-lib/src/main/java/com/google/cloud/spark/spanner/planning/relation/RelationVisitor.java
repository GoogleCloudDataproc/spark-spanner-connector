package com.google.cloud.spark.spanner.planning.relation;

public interface RelationVisitor<T> {
  T visit(TableRelation relation);

  T visit(JoinRelation relation);
}
