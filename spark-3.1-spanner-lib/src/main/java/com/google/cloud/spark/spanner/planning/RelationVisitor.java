package com.google.cloud.spark.spanner.planning;

public interface RelationVisitor<T> {
  T visit(TableRelation relation);
  T visit(JoinRelation relation);
}
