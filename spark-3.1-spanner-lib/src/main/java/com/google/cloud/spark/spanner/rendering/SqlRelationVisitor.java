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
package com.google.cloud.spark.spanner.rendering;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spark.spanner.SpannerInformationSchema;
import com.google.cloud.spark.spanner.binding.ParameterRegistry;
import com.google.cloud.spark.spanner.planning.expression.LiteralExpr;
import com.google.cloud.spark.spanner.planning.relation.JoinRelation;
import com.google.cloud.spark.spanner.planning.relation.JoinType;
import com.google.cloud.spark.spanner.planning.relation.RelationVisitor;
import com.google.cloud.spark.spanner.planning.relation.TableRelation;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class SqlRelationVisitor implements RelationVisitor<RenderResult> {

  private SqlExprVisitor sqlExprVisitor;
  SpannerInformationSchema infoSchema;
  boolean isPostgreSql;

  public SqlRelationVisitor(Dialect dialect) {
    this(dialect, ParameterRegistry.create(dialect));
  }

  public SqlRelationVisitor(Dialect dialect, ParameterRegistry parameterRegistry) {
    this.infoSchema = SpannerInformationSchema.create(dialect);
    this.sqlExprVisitor = SqlExprVisitor.create(dialect, parameterRegistry);
    this.isPostgreSql = (dialect == Dialect.POSTGRESQL);
  }

  @Override
  public RenderResult visit(TableRelation relation) {
    StringBuilder sb = new StringBuilder(infoSchema.quoteIdentifier(relation.getTableName()));

    final String hint = relation.formatIndexHint(isPostgreSql);
    if (hint != null) {
      sb.append(" " + hint); // Add safe whitespace
    }

    if (relation.getAlias() != null) {
      sb.append(" AS " + infoSchema.quoteIdentifier(relation.getAlias()));
    }

    return new RenderResult(sb.toString(), Collections.emptyMap());
  }

  @Override
  public RenderResult visit(JoinRelation relation) {
    RenderResult left = relation.getLeft().accept(this);
    RenderResult right = relation.getRight().accept(this);
    RenderResult condition = relation.getCondition().accept(sqlExprVisitor);
    Map<String, LiteralExpr> bindings = new LinkedHashMap<>();
    bindings.putAll(left.getBindings());
    bindings.putAll(right.getBindings());
    bindings.putAll(condition.getBindings());
    return new RenderResult(
        left.getSql()
            + " "
            + renderJoinType(relation.getJoinType())
            + " "
            + right.getSql()
            + " ON "
            + condition.getSql(),
        bindings);
  }

  private String renderJoinType(JoinType joinType) {
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
