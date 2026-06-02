package com.google.cloud.spark.spanner.planning.relation;

import com.google.cloud.spark.spanner.rendering.RenderResult;

public class SqlRelationVisitor implements RelationVisitor<String> {

  private SqlExprVisitor sqlExprVisitor = new SqlExprVisitor();

  @Override
  public String visit(TableRelation relation) {
    return "SELECT * FROM " + relation.getTableName();
  }

  @Override
  public String visit(JoinRelation relation) {
    String left = relation.getLeft().accept(this);
    String right = relation.getRight().accept(this);
    RenderResult condition = relation.getCondition().accept(sqlExprVisitor);
    return left + " " + renderJoinType(relation.getJoinType()) + " " + right + " ON " + condition;
  }

  private String renderJoinType(JoinType joinType) {
    // TODO: does this need to change for dialects? Do we support CROSS JOIN?
    switch (joinType) {
      case INNER:
        return "INNER JOIN";
      case LEFT_OUTER:
        return "LEFT OUTER JOIN";
      case RIGHT_OUTER:
        return "RIGHT OUTER JOIN";
      case FULL_OUTER:
        return "FULL OUTER JOIN";
    }
    return "";
  }
}
