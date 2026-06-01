package com.google.cloud.spark.spanner.planning;

public final class JoinRelation implements Relation {
  private final Relation left;
  private final Relation right;
  private final JoinType joinType;
  private final BoolExpr condition;

  public JoinRelation(Relation left, Relation right, JoinType joinType) {
    this.left = left;
    this.right = right;
    this.joinType = joinType;
    this.condition = null;
  }

  //  @Override
  //  public String visit(JoinRelation relation) {
  //    String left =
  //        relation.getLeft().accept(this);
  //    String right =
  //        relation.getRight().accept(this);
  //    String condition =
  //        relation.getCondition()
  //            .accept(expressionVisitor);
  //    return left
  //        + " "
  //        + renderJoinType(relation.getJoinType())
  //        + " "
  //        + right
  //        + " ON "
  //        + condition;
  //  }

}
