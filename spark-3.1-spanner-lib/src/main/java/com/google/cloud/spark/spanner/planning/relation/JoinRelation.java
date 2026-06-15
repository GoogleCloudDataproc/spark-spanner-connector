package com.google.cloud.spark.spanner.planning.relation;

import com.google.cloud.spark.spanner.planning.expression.BoolExpr;

public final class JoinRelation implements Relation {
  private final Relation left;
  private final Relation right;
  private final JoinType joinType;
  private final BoolExpr condition;

  public BoolExpr getCondition() {
    return condition;
  }

  public JoinType getJoinType() {
    return joinType;
  }

  public Relation getRight() {
    return right;
  }

  public Relation getLeft() {
    return left;
  }

  public JoinRelation(Relation left, Relation right, JoinType joinType, BoolExpr condition) {
    this.left = left;
    this.right = right;
    this.joinType = joinType;
    this.condition = condition;
  }

  @Override
  public <T> T accept(RelationVisitor<T> visitor) {
    return visitor.visit(this);
  }
}
