package com.google.cloud.spark.spanner.planning.relation;

import static com.google.cloud.spanner.Dialect.GOOGLE_STANDARD_SQL;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;

import com.google.cloud.spark.spanner.planning.expression.*;
import com.google.cloud.spark.spanner.rendering.RenderResult;
import com.google.cloud.spark.spanner.rendering.SqlRelationVisitor;
import com.google.cloud.spark.spanner.scan.SpannerTable;
import java.util.Map;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Test;

public class RelationTableTest {

  Map<String, String> options;
  SpannerTable spannerTable = mock(SpannerTable.class);

  @Test
  public void testTableRelation() {
    Relation relation = new TableRelation("ATable", "a", spannerTable);

    SqlRelationVisitor visitor = new SqlRelationVisitor(GOOGLE_STANDARD_SQL);

    String result = relation.accept(visitor).getSql();

    assertThat((String) result).isEqualTo("`ATable` AS `a`");
  }

  @Test
  public void testJoinRelation() {
    final LiteralExpr aStringLiteral = new LiteralExpr("test", DataTypes.StringType);
    BoolExpr expr = new EqExpr(new ColumnExpr("aCol", DataTypes.StringType, false), aStringLiteral);
    Relation tableA = new TableRelation("ATable", "a", spannerTable);
    Relation tableB = new TableRelation("BTable", "b", spannerTable);
    Relation relation = new JoinRelation(tableA, tableB, JoinType.INNER, expr);

    SqlRelationVisitor visitor = new SqlRelationVisitor(GOOGLE_STANDARD_SQL);

    RenderResult result = relation.accept(visitor);

    assertThat((String) result.getSql())
        .isEqualTo("`ATable` AS `a` INNER JOIN `BTable` AS `b` ON `aCol` = @p1");
    assertThat(result.getBindings().get("p1")).isEqualTo(aStringLiteral);
  }
}
