package com.google.cloud.spark.spanner.planning.expression;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.spark.spanner.rendering.RenderResult;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Test;

public class SpannerExprTest {

  @Test
  public void testVisit() {
    SpannerExpr predicate =
        new OrExpr(
            new AndExpr(
                new EqExpr(
                    new ColumnExpr("SingerId"),
                    new LiteralExpr(Long.valueOf(123L), DataTypes.LongType)),
                new EqExpr(
                    new ColumnExpr("Active"),
                    new LiteralExpr(Boolean.valueOf(true), DataTypes.BooleanType))),
            new TrueExpr());

    SqlExprVisitor visitor = new SqlExprVisitor();

    RenderResult result = predicate.accept(visitor);
    assertThat(result.getSql()).isEqualTo("((SingerId = @p1 AND Active = @p2) OR TRUE)");
    assertThat((long) (result.getBindings().get("p1"))).isEqualTo(123L);
  }
}
