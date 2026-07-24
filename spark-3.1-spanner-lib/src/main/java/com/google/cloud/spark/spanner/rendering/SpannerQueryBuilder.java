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
import com.google.cloud.spanner.Statement;
import com.google.cloud.spark.spanner.SpannerConnectorException;
import com.google.cloud.spark.spanner.SpannerErrorCode;
import com.google.cloud.spark.spanner.SparkFilterUtils;
import com.google.cloud.spark.spanner.binding.SpannerTypeBinder;
import com.google.cloud.spark.spanner.planning.expression.LiteralExpr;
import com.google.cloud.spark.spanner.planning.query.LogicalQuery;
import com.google.cloud.spark.spanner.planning.relation.Relation;
import com.google.cloud.spark.spanner.planning.relation.TableRelation;
import com.google.cloud.spark.spanner.scan.SpannerTable;
import java.util.*;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerQueryBuilder {
  private static final Logger logger = LoggerFactory.getLogger(SpannerQueryBuilder.class);

  private final LogicalQuery logicalQuery;
  private final Dialect dialect;
  private final boolean enablePredicateSql;

  private SpannerQueryBuilder(
      LogicalQuery logicalQuery, Dialect dialect, boolean enablePredicateSql) {
    this.logicalQuery = logicalQuery;
    this.dialect = dialect;
    this.enablePredicateSql = enablePredicateSql;
  }

  public static SpannerQueryBuilder newBuilder(
      LogicalQuery logicalQuery, Dialect dialect, boolean enablePredicateSql) {
    return new SpannerQueryBuilder(logicalQuery, dialect, enablePredicateSql);
  }

  private RenderResult buildSql() {
    final boolean isPostgreSql = this.dialect.equals(Dialect.POSTGRESQL);
    Relation relation = logicalQuery.getSource();
    String alias = null;
    if (relation instanceof TableRelation) {
      TableRelation tableRelation = (TableRelation) relation;
      alias = tableRelation.getAlias();
    }

    // 1. Use * if no requiredColumns were requested else select them.
    String selectPrefix = "SELECT *";
    if (this.logicalQuery.getProjections() != null
        && !this.logicalQuery.getProjections().isEmpty()) {
      // Prefix each column with the table name to avoid ambiguity when column name
      // matches table name
      String columnsWithTablePrefix =
          buildColumnsWithTablePrefix(
              alias,
              new LinkedHashSet<>(this.logicalQuery.getProjections()),
              dialect.equals(Dialect.POSTGRESQL));
      selectPrefix = "SELECT " + columnsWithTablePrefix;
    }

    SqlRelationVisitor relationVisitor = new SqlRelationVisitor(this.dialect);
    String query = selectPrefix + " FROM ";
    RenderResult result = logicalQuery.getSource().accept(relationVisitor);
    query += result.getSql();

    Map<String, LiteralExpr> bindings = new HashMap<>();
    bindings.putAll(result.getBindings());

    if (this.logicalQuery.getFilter().length > 0) {
      query +=
          " WHERE "
              + SparkFilterUtils.getCompiledFilter(
                  true,
                  Optional.empty(),
                  isPostgreSql,
                  this.logicalQuery.getFields(),
                  this.logicalQuery.getFilter());
    }

    logger.debug("query: {}", query);
    for (Map.Entry<String, LiteralExpr> entry : bindings.entrySet()) {
      logger.debug(
          "bindings: Key: {}, Value type: {}: {}",
          entry.getKey(),
          entry.getValue().getSparkType().toString(),
          entry.getValue().getValue().toString());
    }

    return new RenderResult(query, bindings);
  }

  private Statement buildNewStatement() {
    RenderResult renderResult = this.buildSql();
    logger.info("buildNewStatement renderResult: {}", renderResult.getSql());
    Statement.Builder builder = Statement.newBuilder(renderResult.getSql());
    Map<String, LiteralExpr> bindings = renderResult.getBindings();
    if (bindings != null) {
      for (Map.Entry<String, LiteralExpr> entry : bindings.entrySet()) {
        SpannerTypeBinder.bind(builder, entry.getKey(), entry.getValue());
      }
    }
    return builder.build();
  }

  public Statement buildStatement() {
    if (enablePredicateSql) {
      return buildNewStatement();
    } else {
      return buildLegacySql();
    }
  }

  public static String buildColumnsWithTablePrefix(
      String tableName, Set<String> columns, boolean isPostgreSql) {
    String quotedTableName = isPostgreSql ? "\"" + tableName + "\"" : "`" + tableName + "`";
    return columns.stream()
        .map(col -> isPostgreSql ? "\"" + col + "\"" : "`" + col + "`")
        .map(quotedCol -> quotedTableName + "." + quotedCol)
        .collect(Collectors.joining(", "));
  }

  private String parenthesize(String in) {
    return "(" + in + ")";
  }

  private Statement buildLegacySql() {
    boolean isPostgreSql = this.dialect.equals(Dialect.POSTGRESQL);
    Relation relation = this.logicalQuery.getSource();
    if (!(relation instanceof TableRelation)) {
      throw new SpannerConnectorException(
          SpannerErrorCode.INVALID_ARGUMENT,
          "Spanner Table not defined for legacy SQL generation.");
    }
    SpannerTable spannerTable = ((TableRelation) relation).getTable();

    // 1. Use * if no requiredColumns were requested else select them.
    String selectPrefix = "SELECT *";
    if (this.logicalQuery.getProjections() != null
        && !this.logicalQuery.getProjections().isEmpty()) {
      // Prefix each column with the table name to avoid ambiguity when column name
      // matches table name
      String columnsWithTablePrefix =
          buildColumnsWithTablePrefix(
              spannerTable.name(), this.logicalQuery.getProjections(), isPostgreSql);
      selectPrefix = "SELECT " + columnsWithTablePrefix;
    }

    String quotedTableName =
        isPostgreSql ? "\"" + spannerTable.name() + "\"" : "`" + spannerTable.name() + "`";
    String sqlStmt = selectPrefix + " FROM " + quotedTableName;
    if (this.logicalQuery.getFilter().length > 0) {
      sqlStmt +=
          " WHERE "
              + SparkFilterUtils.getCompiledFilter(
                  true,
                  Optional.empty(),
                  isPostgreSql,
                  this.logicalQuery.getFields(),
                  this.logicalQuery.getFilter());
    }
    return Statement.of(sqlStmt);
  }
}
