// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.google.cloud.spark.spanner.planning.expression;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spark.spanner.rendering.RenderResult;
import com.google.cloud.spark.spanner.rendering.SqlExprVisitor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Test;

public class SpannerExprTest {

  @Test
  public void testVisit() {
    final LiteralExpr aLongLiteral =
        new LiteralExpr(Long.valueOf(123L), DataTypes.LongType, "INT64");
    final LiteralExpr aBooleanLiteral =
        new LiteralExpr(Boolean.valueOf(true), DataTypes.BooleanType, "BOOL");
    SpannerExpr predicate =
        new OrExpr(
            new AndExpr(
                new EqExpr(
                    new ColumnExpr("ATable", "SingerId", DataTypes.LongType, false), aLongLiteral),
                new EqExpr(
                    new ColumnExpr("ATable", "Active", DataTypes.BooleanType, false),
                    aBooleanLiteral)),
            new TrueExpr());

    SqlExprVisitor visitor = SqlExprVisitor.create(Dialect.GOOGLE_STANDARD_SQL);

    RenderResult result = predicate.accept(visitor);
    assertThat(result.getSql())
        .isEqualTo("((`ATable`.`SingerId` = @p1 AND `ATable`.`Active` = @p2) OR TRUE)");
    assertThat(result.getBindings().get("p1")).isEqualTo(aLongLiteral);
    assertThat(result.getBindings().get("p2")).isEqualTo(aBooleanLiteral);
  }

  @Test
  public void testArithmeticVisit() {
    final LiteralExpr a47Literal = new LiteralExpr(Long.valueOf(47), DataTypes.LongType);
    final LiteralExpr a45Literal = new LiteralExpr(Long.valueOf(45), DataTypes.LongType);
    final LiteralExpr a43Literal = new LiteralExpr(Long.valueOf(43), DataTypes.LongType);
    SpannerExpr predicate =
        new EqExpr(
            new ColumnExpr("ATable", "SingerId", DataTypes.LongType, false),
            new ArithmeticExpr(
                a47Literal,
                ArithmeticExpr.Operator.ADD,
                new ArithmeticExpr(a43Literal, ArithmeticExpr.Operator.MULTIPLY, a45Literal)));

    SqlExprVisitor visitor = SqlExprVisitor.create(Dialect.GOOGLE_STANDARD_SQL);

    RenderResult result = predicate.accept(visitor);
    assertThat(result.getSql()).isEqualTo("`ATable`.`SingerId` = (@p1+(@p2*@p3))");
    assertThat(result.getBindings().get("p1")).isEqualTo(a47Literal);
    assertThat(result.getBindings().get("p2")).isEqualTo(a43Literal);
    assertThat(result.getBindings().get("p3")).isEqualTo(a45Literal);
  }

  @Test
  public void testInVisit() {
    final LiteralExpr a47Literal = new LiteralExpr(Long.valueOf(47), DataTypes.LongType, "INT64");
    final LiteralExpr a45Literal = new LiteralExpr(Long.valueOf(45), DataTypes.LongType, "INT64");
    final LiteralExpr a43Literal = new LiteralExpr(Long.valueOf(43), DataTypes.LongType, "INT64");
    SpannerExpr predicate =
        new InExpr(
            new ColumnExpr("ATable", "SingerId", DataTypes.LongType, false),
            new ArrayList<>(Arrays.asList(a43Literal, a47Literal, a45Literal)));

    SqlExprVisitor visitor = SqlExprVisitor.create(Dialect.GOOGLE_STANDARD_SQL);

    RenderResult result = predicate.accept(visitor);
    assertThat(result.getSql()).isEqualTo("`ATable`.`SingerId` IN (@p1, @p2, @p3)");
    assertThat(result.getBindings().get("p1")).isEqualTo(a43Literal);
    assertThat(result.getBindings().get("p2")).isEqualTo(a47Literal);
    assertThat(result.getBindings().get("p3")).isEqualTo(a45Literal);
  }

  @Test
  public void testPostgresLikeExpressionsEscapeWildcards() {
    ColumnExpr col = new ColumnExpr("ATable", "Name", DataTypes.StringType, false);
    LiteralExpr pattern = new LiteralExpr("foo%_\\bar", DataTypes.StringType, "STRING");

    SqlExprVisitor startsWithVisitor = SqlExprVisitor.create(Dialect.POSTGRESQL);
    RenderResult startsWithResult = new StartsWithExpr(col, pattern).accept(startsWithVisitor);
    assertThat(startsWithResult.getSql())
        .isEqualTo(
            "\"ATable\".\"Name\" LIKE (REPLACE(REPLACE(REPLACE($1, '\\', '\\\\'), '%', '\\%'), '_', '\\_') || '%')");
    assertThat(startsWithResult.getBindings().get("p1")).isEqualTo(pattern);

    SqlExprVisitor endsWithVisitor = SqlExprVisitor.create(Dialect.POSTGRESQL);
    RenderResult endsWithResult = new EndsWithExpr(col, pattern).accept(endsWithVisitor);
    assertThat(endsWithResult.getSql())
        .isEqualTo(
            "\"ATable\".\"Name\" LIKE ('%' || REPLACE(REPLACE(REPLACE($1, '\\', '\\\\'), '%', '\\%'), '_', '\\_'))");
    assertThat(endsWithResult.getBindings().get("p1")).isEqualTo(pattern);

    SqlExprVisitor containsVisitor = SqlExprVisitor.create(Dialect.POSTGRESQL);
    RenderResult containsResult = new ContainsExpr(col, pattern).accept(containsVisitor);
    assertThat(containsResult.getSql())
        .isEqualTo(
            "\"ATable\".\"Name\" LIKE ('%' || REPLACE(REPLACE(REPLACE($1, '\\', '\\\\'), '%', '\\%'), '_', '\\_') || '%')");
    assertThat(containsResult.getBindings().get("p1")).isEqualTo(pattern);
  }

  @Test
  public void testEqNullSafeInsideAndExprWrapsDisjunctionInParentheses() {
    ColumnExpr deletedAt = new ColumnExpr("ATable", "DeletedAt", DataTypes.LongType, true);
    ColumnExpr tenantId = new ColumnExpr("ATable", "TenantId", DataTypes.StringType, false);
    LiteralExpr nullLong = new LiteralExpr(null, DataTypes.LongType, "INT64");
    LiteralExpr tenantVal = new LiteralExpr("tenant_A", DataTypes.StringType, "STRING");

    SpannerExpr expr =
        new AndExpr(new EqNullSafeExpr(deletedAt, nullLong), new EqExpr(tenantId, tenantVal));
    RenderResult result = expr.accept(SqlExprVisitor.create(Dialect.GOOGLE_STANDARD_SQL));
    assertThat(result.getSql())
        .isEqualTo(
            "(((`ATable`.`DeletedAt` IS NULL AND @p1 IS NULL) OR (`ATable`.`DeletedAt` = @p1)) AND `ATable`.`TenantId` = @p2)");
  }

  @Test
  public void testEmptyInExprRendersFalse() {
    ColumnExpr col = new ColumnExpr("ATable", "SingerId", DataTypes.LongType, false);
    RenderResult result =
        new InExpr(col, Collections.emptyList())
            .accept(SqlExprVisitor.create(Dialect.GOOGLE_STANDARD_SQL));
    assertThat(result.getSql()).isEqualTo("FALSE");
    assertThat(result.getBindings()).isEmpty();
  }

  @Test
  public void testColumnExprWithEmptyOrNullTableAliasOmitsPrefix() {
    ColumnExpr emptyAliasCol = new ColumnExpr("", "SingerId", DataTypes.LongType, false);
    ColumnExpr nullAliasCol = new ColumnExpr(null, "SingerId", DataTypes.LongType, false);

    assertThat(emptyAliasCol.accept(SqlExprVisitor.create(Dialect.GOOGLE_STANDARD_SQL)).getSql())
        .isEqualTo("`SingerId`");
    assertThat(nullAliasCol.accept(SqlExprVisitor.create(Dialect.POSTGRESQL)).getSql())
        .isEqualTo("\"SingerId\"");
  }
}
