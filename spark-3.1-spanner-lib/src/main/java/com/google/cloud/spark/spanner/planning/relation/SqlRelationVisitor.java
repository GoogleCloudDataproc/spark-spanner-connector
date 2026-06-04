package com.google.cloud.spark.spanner.planning.relation;

import com.google.cloud.spark.spanner.planning.expression.SqlExprVisitor;
import com.google.cloud.spark.spanner.rendering.RenderResult;
import java.util.Collections;

public class SqlRelationVisitor implements RelationVisitor<RenderResult> {

  private SqlExprVisitor sqlExprVisitor = new SqlExprVisitor();

  @Override
  public RenderResult visit(TableRelation relation) {
    return new RenderResult(
        relation.getTableName() + " " + relation.getAlias(), Collections.emptyMap());
  }

  @Override
  public RenderResult visit(JoinRelation relation) {
    String left = relation.getLeft().accept(this).getSql();
    String right = relation.getRight().accept(this).getSql();
    RenderResult condition = relation.getCondition().accept(sqlExprVisitor);
    return new RenderResult(
        left
            + " "
            + renderJoinType(relation.getJoinType())
            + " "
            + right
            + " ON "
            + condition.getSql(),
        condition.getBindings());
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
