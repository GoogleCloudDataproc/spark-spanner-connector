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
package com.google.cloud.spark.spanner.planning.relation;

import static com.google.cloud.spanner.Dialect.GOOGLE_STANDARD_SQL;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spark.spanner.planning.expression.*;
import com.google.cloud.spark.spanner.rendering.RenderResult;
import com.google.cloud.spark.spanner.rendering.SqlRelationVisitor;
import com.google.cloud.spark.spanner.scan.SpannerTable;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.Test;

public class RelationTableTest {

  SpannerTable mockSpannerTable = mock(SpannerTable.class);
  CaseInsensitiveStringMap mockProperties = mock(CaseInsensitiveStringMap.class);

  @Test
  public void testTableRelation() {
    Relation relation = new TableRelation("ATable", "a", mockSpannerTable);
    SqlRelationVisitor visitor = new SqlRelationVisitor(GOOGLE_STANDARD_SQL);

    when(mockSpannerTable.properties()).thenReturn(mockProperties);
    when(mockProperties.get("indexHint")).thenReturn(null);

    String result = relation.accept(visitor).getSql();

    assertThat(result).isEqualTo("`ATable` AS `a`");
  }

  @Test
  public void testJoinRelation() {
    final LiteralExpr aStringLiteral = new LiteralExpr("test", DataTypes.StringType);
    BoolExpr expr =
        new EqExpr(new ColumnExpr("a", "aCol", DataTypes.StringType, false), aStringLiteral);
    Relation tableA = new TableRelation("ATable", "a", mockSpannerTable);
    Relation tableB = new TableRelation("BTable", "b", mockSpannerTable);
    Relation relation = new JoinRelation(tableA, tableB, JoinType.INNER, expr);

    SqlRelationVisitor visitor = new SqlRelationVisitor(GOOGLE_STANDARD_SQL);

    when(mockSpannerTable.properties()).thenReturn(mockProperties);
    when(mockProperties.get("indexHint")).thenReturn(null);

    RenderResult result = relation.accept(visitor);

    assertThat((String) result.getSql())
        .isEqualTo("`ATable` AS `a` INNER JOIN `BTable` AS `b` ON `a`.`aCol` = @p1");
    assertThat(result.getBindings().get("p1")).isEqualTo(aStringLiteral);
  }

  @Test
  public void testJoinRelationWithHint() {
    final LiteralExpr aStringLiteral = new LiteralExpr("test", DataTypes.StringType);
    BoolExpr expr =
        new EqExpr(new ColumnExpr("a", "aCol", DataTypes.StringType, false), aStringLiteral);
    Relation tableA = new TableRelation("ATable", "a", mockSpannerTable);
    Relation tableB = new TableRelation("BTable", "b", mockSpannerTable);
    Relation relation = new JoinRelation(tableA, tableB, JoinType.INNER, expr);

    SqlRelationVisitor visitor = new SqlRelationVisitor(GOOGLE_STANDARD_SQL);

    when(mockSpannerTable.properties()).thenReturn(mockProperties);
    when(mockProperties.get("indexHint")).thenReturn("my_index");

    RenderResult result = relation.accept(visitor);

    assertThat((String) result.getSql())
        .isEqualTo(
            "`ATable` @{FORCE_INDEX=my_index} AS `a` INNER JOIN `BTable` @{FORCE_INDEX=my_index} AS `b` ON `a`.`aCol` = @p1");
    assertThat(result.getBindings().get("p1")).isEqualTo(aStringLiteral);
  }
}
