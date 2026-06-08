package com.google.cloud.spark.spanner.rendering;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spark.spanner.SpannerInformationSchema;
import com.google.cloud.spark.spanner.planning.relation.JoinRelation;
import com.google.cloud.spark.spanner.planning.relation.JoinType;
import com.google.cloud.spark.spanner.planning.relation.RelationVisitor;
import com.google.cloud.spark.spanner.planning.relation.TableRelation;
import java.util.Collections;

public class SqlRelationVisitor implements RelationVisitor<RenderResult> {

  private SqlExprVisitor sqlExprVisitor;
  SpannerInformationSchema infoSchema;

  public SqlRelationVisitor(Dialect dialect) {
    this.infoSchema = SpannerInformationSchema.create(dialect);
    this.sqlExprVisitor = new SqlExprVisitor(dialect);
  }

  @Override
  public RenderResult visit(TableRelation relation) {
    StringBuilder sb = new StringBuilder(infoSchema.quoteIdentifier(relation.getTableName()));
    if (relation.getAlias() != null) {
      sb.append(" AS " + infoSchema.quoteIdentifier(relation.getAlias()));
    }
    return new RenderResult(sb.toString(), Collections.emptyMap());
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
