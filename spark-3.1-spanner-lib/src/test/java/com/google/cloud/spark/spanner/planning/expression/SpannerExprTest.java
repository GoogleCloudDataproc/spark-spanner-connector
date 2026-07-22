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

    SqlExprVisitor visitor = SqlExprVisitor.create(Dialect.GOOGLE_STANDARD_SQL);

    RenderResult result = predicate.accept(visitor);
    assertThat(result.getSql()).isEqualTo("`SingerId` = @p1 AND `Active` = @p2 OR TRUE");
    assertThat(result.getBindings().get("p1")).isEqualTo(aLongLiteral);
    assertThat(result.getBindings().get("p2")).isEqualTo(aBooleanLiteral);
  }
}
