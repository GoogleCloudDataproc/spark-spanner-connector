package com.google.cloud.spark.spanner.planning.expression;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spark.spanner.rendering.RenderResult;
import com.google.cloud.spark.spanner.rendering.SqlExprVisitor;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Test;

public class SpannerExprTest {

  @Test
  public void testVisit() {
    final LiteralExpr aLongLiteral = new LiteralExpr(Long.valueOf(123L), DataTypes.LongType);
    final LiteralExpr aBooleanLiteral =
        new LiteralExpr(Boolean.valueOf(true), DataTypes.BooleanType);
    SpannerExpr predicate =
        new OrExpr(
            new AndExpr(
                new EqExpr(new ColumnExpr("SingerId", DataTypes.LongType, false), aLongLiteral),
                new EqExpr(
                    new ColumnExpr("Active", DataTypes.BooleanType, false), aBooleanLiteral)),
            new TrueExpr());

    SqlExprVisitor visitor = new SqlExprVisitor(Dialect.GOOGLE_STANDARD_SQL);

    RenderResult result = predicate.accept(visitor);
    assertThat(result.getSql()).isEqualTo("`SingerId` = @p1 AND `Active` = @p2 OR TRUE");
    assertThat(result.getBindings().get("p1")).isEqualTo(aLongLiteral);
    assertThat(result.getBindings().get("p2")).isEqualTo(aBooleanLiteral);
  }
}
