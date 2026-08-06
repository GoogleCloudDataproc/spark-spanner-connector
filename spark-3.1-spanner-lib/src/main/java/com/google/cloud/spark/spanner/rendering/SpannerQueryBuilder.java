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
import java.util.stream.Stream;
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
    logger.info("buildSql");
    final boolean isPostgreSql = this.dialect.equals(Dialect.POSTGRESQL);
    String alias = null;
    String aliasOther = null;
    if (logicalQuery.sourceIsTable()) {
      alias = logicalQuery.getTableAlias();
      logger.info("TableRelation found alias: {}", alias);
    } else if (logicalQuery.sourceIsJoin()) {
      alias = logicalQuery.getTableAlias();
      aliasOther = logicalQuery.getOtherTableAlias();
      logger.info("JoinRelation found alias: {}, aliasOther: {}", alias, aliasOther);
    }

    // 1. Use * if no requiredColumns were requested else select them.
    String selectPrefix = "SELECT *";
    String columnsWithTablePrefix = null;
    String otherColumnsWithTablePrefix = null;
    if (this.logicalQuery.hasRequiredColumns()) {
      logger.info("hasRequiredColumns()");
      // Prefix each column with the table name to avoid ambiguity when column name
      // matches table name
      columnsWithTablePrefix =
          buildColumnsWithTablePrefix(
              alias, this.logicalQuery.getRequiredColumnsForSelect(), isPostgreSql);
      selectPrefix = "SELECT " + columnsWithTablePrefix;
      logger.info("hasRequiredColumns() selectPrefix: {}", selectPrefix);
    }

    if (this.logicalQuery.hasOtherRequiredColumns()) {
      logger.info("hasOtherRequiredColumns()");
      otherColumnsWithTablePrefix =
          buildColumnsWithTablePrefix(
              aliasOther, this.logicalQuery.getOtherRequiredColumnsForSelect(), isPostgreSql);
      logger.info(
          "hasOtherRequiredColumns() otherColumnsWithTablePrefix: {}", otherColumnsWithTablePrefix);
    }
    final String columns =
        Stream.of(columnsWithTablePrefix, otherColumnsWithTablePrefix)
            .filter(Objects::nonNull)
            .filter(s -> !s.isEmpty()) // Optional: removes empty strings too
            .collect(Collectors.joining(","));

    if (!columns.isEmpty()) {
      selectPrefix = "SELECT " + columns;
      logger.info("hasOtherRequiredColumns() selectPrefix: {}", selectPrefix);
    }
    logger.info("!columns.isEmpty(): {}", selectPrefix);

    SqlRelationVisitor relationVisitor = new SqlRelationVisitor(this.dialect);
    String query = selectPrefix + " FROM ";
    RenderResult result = logicalQuery.getSource().accept(relationVisitor);
    query += result.getSql();

    Map<String, LiteralExpr> bindings = new HashMap<>();
    bindings.putAll(result.getBindings());

    if (this.logicalQuery.getFilter().length > 0 || this.logicalQuery.getFilterOther().length > 0) {
      logger.info("buildSql: at least one filter found");
      String leftFilter = null;
      String otherFilter = null;
      if (this.logicalQuery.getFilter().length > 0) {
        leftFilter =
            SparkFilterUtils.getCompiledFilter(
                true,
                Optional.empty(),
                isPostgreSql,
                this.logicalQuery.getFields(),
                alias,
                this.logicalQuery.getFilter());
        logger.info("buildSql: alias: {}, left filter: {}", alias, leftFilter);
      }
      if (this.logicalQuery.getFilterOther().length > 0) {
        otherFilter =
            SparkFilterUtils.getCompiledFilter(
                true,
                Optional.empty(),
                isPostgreSql,
                this.logicalQuery.getFields(),
                aliasOther,
                this.logicalQuery.getFilterOther());
        logger.info("buildSql: aliasOther: {}, other filter: {}", aliasOther, otherFilter);
      }
      String filterStr =
          Stream.of(leftFilter, otherFilter)
              .filter(Objects::nonNull)
              .filter(s -> !s.isEmpty()) // Optional: removes empty strings too
              .collect(Collectors.joining(" AND "));
      query += " WHERE " + filterStr;
      logger.info("buildSql: query: {}", query);
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
      String tableName, List<String> columns, boolean isPostgreSql) {
    String quotedTableName = isPostgreSql ? "\"" + tableName + "\"" : "`" + tableName + "`";
    return columns.stream()
        .map(col -> isPostgreSql ? "\"" + col + "\"" : "`" + col + "`")
        .map(quotedCol -> quotedTableName + "." + quotedCol)
        .collect(Collectors.joining(", "));
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
    if (this.logicalQuery.hasRequiredColumns()) {
      // Prefix each column with the table name to avoid ambiguity when column name
      // matches table name
      String columnsWithTablePrefix =
          buildColumnsWithTablePrefix(
              spannerTable.name(), this.logicalQuery.getRequiredColumnsForSelect(), isPostgreSql);
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
