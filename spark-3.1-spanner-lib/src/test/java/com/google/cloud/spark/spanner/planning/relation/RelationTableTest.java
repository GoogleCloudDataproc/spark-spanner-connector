package com.google.cloud.spark.spanner.planning.relation;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.spark.spanner.planning.expression.*;
import com.google.cloud.spark.spanner.rendering.RenderResult;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Test;

public class RelationTableTest {

  @Test
  public void testTableRelation() {
    Relation relation = new TableRelation("ATable", "a");

    SqlRelationVisitor visitor = new SqlRelationVisitor();

    String result = relation.accept(visitor).getSql();

    assertThat((String) result).isEqualTo("ATable a");
  }

  @Test
  public void testJoinRelation() {
    Relation tableA = new TableRelation("ATable", "a");
    Relation tableB = new TableRelation("BTable", "b");
    BoolExpr expr =
        new EqExpr(
            new ColumnExpr("aCol", DataTypes.StringType, false),
            new LiteralExpr("test", DataTypes.StringType));
    Relation relation = new JoinRelation(tableA, tableB, JoinType.INNER, expr);

    SqlRelationVisitor visitor = new SqlRelationVisitor();

    RenderResult result = relation.accept(visitor);

    assertThat((String) result.getSql()).isEqualTo("ATable a INNER JOIN BTable b ON aCol = @p1");
    assertThat((String) (result.getBindings().get("p1"))).isEqualTo("test");
  }
}
