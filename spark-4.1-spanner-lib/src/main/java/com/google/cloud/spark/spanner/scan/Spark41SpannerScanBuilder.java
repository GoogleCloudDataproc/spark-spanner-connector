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

package com.google.cloud.spark.spanner.scan;

import com.google.cloud.spark.spanner.planning.expression.BoolExpr;
import com.google.cloud.spark.spanner.planning.query.ColumnResolution;
import com.google.cloud.spark.spanner.planning.query.PredicateToExprConverter;
import com.google.cloud.spark.spanner.planning.relation.JoinRelation;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.join.JoinType;
import org.apache.spark.sql.connector.read.SupportsPushDownJoin;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Spark41SpannerScanBuilder extends SpannerScanBuilder implements SupportsPushDownJoin {
  private static final Logger logger = LoggerFactory.getLogger(Spark41SpannerScanBuilder.class);
  private boolean enablePredicateSql = false;

  public Spark41SpannerScanBuilder(SpannerTable spannerTable) {
    super(spannerTable);
    logger.info("Spark41SpannerScanBuilder created");

    final CaseInsensitiveStringMap opts = spannerTable.properties();
    if (opts.containsKey("enablePredicateSql")) {
      enablePredicateSql = opts.get("enablePredicateSql").equalsIgnoreCase("true");
      logger.info("Enable Predicate Sql: {}", enablePredicateSql);
    }
  }

  public boolean isOtherSideCompatibleForJoin(SupportsPushDownJoin other) {
    logger.info("isOtherSideCompatibleForJoin: {}", other);
    if (!(other instanceof Spark41SpannerScanBuilder)) {
      return false;
    }
    if (!enablePredicateSql) {
      return false;
    }

    Spark41SpannerScanBuilder otherScan = (Spark41SpannerScanBuilder) other;

    boolean isCompatible =
        this.getDatabaseId().equals(otherScan.getDatabaseId())
            && this.getInstanceId().equals(otherScan.getInstanceId());
    logger.info("isCompatible: {}", isCompatible);
    return isCompatible;
  }

  public boolean pushDownJoin(
      SupportsPushDownJoin other,
      JoinType joinType,
      ColumnWithAlias[] leftSideRequiredColumnsWithAliases,
      ColumnWithAlias[] rightSideRequiredColumnsWithAliases,
      Predicate predicate) {
    logger.info("pushDownJoin called");
    logger.info("this={}", System.identityHashCode(this));
    logger.info("other={}", System.identityHashCode(other));

    if (!(other instanceof Spark41SpannerScanBuilder)) {
      logger.error("pushDownJoin: other is not a SpannerScanBuilder");
      return false;
    }
    Spark41SpannerScanBuilder right = (Spark41SpannerScanBuilder) other;

    if (!isJoinTypeAllowed(joinType)) {
      logger.error("pushDownJoin: join type is not allowed");
      return false;
    }

    if (!isInterleavedJoin(right)) {
      logger.error("pushDownJoin: right is not interleaved");
      return false;
    }

    // Combine the schema of the left and right required columns.
    // This is the schema of the ON clause.
    logger.info("pushDownJoin: leftSideRequiredColumnsWithAliases");
    StructType joinSchema =
        calculateJoinOutputSchema(leftSideRequiredColumnsWithAliases, this.getSchema());

    logger.info("pushDownJoin: rightSideRequiredColumnsWithAliases");
    joinSchema =
        joinSchema.merge(
            calculateJoinOutputSchema(rightSideRequiredColumnsWithAliases, right.getSchema()),
            false);

    logger.info("pushDownJoin: joinSchema: {}", joinSchema);

    final Map<String, ColumnResolution> resolutionMap =
        createColumnResolutionMap(
            this.getTableName(),
            leftSideRequiredColumnsWithAliases,
            this.getSchema(),
            right.getTableName(),
            rightSideRequiredColumnsWithAliases,
            right.getSchema());

    try {
      logger.debug("predicate class = {}", predicate.getClass());
      logger.debug("predicate = {}", predicate);

      BoolExpr condition = PredicateToExprConverter.translatePredicate(predicate, resolutionMap);

      JoinRelation joinRelation =
          new JoinRelation(
              this.createTableRelation(),
              right.createTableRelation(),
              sparkToConnector(joinType),
              condition);

      // For joins, join condition and filter condition are not sent to the same SpannerScanBuiler
      // by Spark.
      // Keep both tables in the join so complete SQL rendering can be done in a single object.
      setJoin(joinRelation, joinSchema, resolutionMap, right);
    } catch (UnsupportedOperationException e) {
      // If predicate conversion fails, fall back to Spark-side execution.
      logger.error("pushDownJoin: predicate conversion fails");
      return false;
    }

    logger.info("pushDownJoin: OK");
    return true;
  }

  private boolean isInterleavedJoin(SpannerScanBuilder other) {
    final InterleaveTableMetadata thisTableMetadata = this.getInterleavedTableMetadata();
    final InterleaveTableMetadata otherTableMetadata = other.getInterleavedTableMetadata();
    final String thisTableParent = thisTableMetadata.getParentTable();
    final String otherTableParent = otherTableMetadata.getParentTable();
    return otherTableParent != null && thisTableMetadata.getTableName().equals(otherTableParent)
        || thisTableParent != null && otherTableMetadata.getTableName().equals(thisTableParent);
  }

  private boolean isJoinTypeAllowed(JoinType joinType) {
    return (joinType == JoinType.INNER_JOIN
        || joinType == JoinType.LEFT_OUTER_JOIN
        || joinType == JoinType.RIGHT_OUTER_JOIN);
  }

  private StructType calculateJoinOutputSchema(
      SupportsPushDownJoin.ColumnWithAlias[] columnsWithAliases, StructType schema) {

    StructType newSchema = new StructType();

    for (SupportsPushDownJoin.ColumnWithAlias columnWithAlias : columnsWithAliases) {
      String columnName = columnWithAlias.colName();
      String alias = columnWithAlias.alias();
      logger.info("calculateJoinOutputSchema: columnName: {}, alias: {}", columnName, alias);

      StructField field = schema.apply(columnName);

      if (alias == null) {
        newSchema = newSchema.add(field);
      } else {
        newSchema = newSchema.add(alias, field.dataType(), field.nullable(), field.metadata());
      }
    }

    return newSchema;
  }

  private static Map<String, ColumnResolution> createColumnResolutionMap(
      String leftTableAlias,
      ColumnWithAlias[] leftColumns,
      StructType leftSchema,
      String rightTableAlias,
      ColumnWithAlias[] rightColumns,
      StructType rightSchema) {
    // Use LinkedHashMap to preserve insertion order.
    // This is important to ensure required columns matches schema order.
    Map<String, ColumnResolution> columnResolutionMap = new LinkedHashMap<>();
    populateColumnResolutionMap(columnResolutionMap, leftColumns, leftTableAlias, leftSchema);
    populateColumnResolutionMap(columnResolutionMap, rightColumns, rightTableAlias, rightSchema);
    return columnResolutionMap;
  }

  private static void populateColumnResolutionMap(
      Map<String, ColumnResolution> columnResolutionMap,
      ColumnWithAlias[] columns,
      String tableAlias,
      StructType schema) {
    logger.info("populateColumnResolutionMap: tableAlias: {}", tableAlias);
    for (SupportsPushDownJoin.ColumnWithAlias columnWithAlias : columns) {
      final String columnName = columnWithAlias.colName();
      final String alias = columnWithAlias.alias() == null ? columnName : columnWithAlias.alias();
      final StructField field = schema.apply(columnName);
      final ColumnResolution columnResolution =
          new ColumnResolution(alias, columnName, tableAlias, field.dataType(), field.nullable());
      logger.debug(
          "populateColumnResolutionMap: alias: {}, columnName: {}, tableAlias: {}, dataType(): {}, field.dataType(): {}, field.nullable(): {}",
          alias,
          columnName,
          tableAlias,
          field.dataType(),
          field.nullable());
      columnResolutionMap.put(alias, columnResolution);
    }
  }

  private com.google.cloud.spark.spanner.planning.relation.JoinType sparkToConnector(
      org.apache.spark.sql.connector.join.JoinType sparkJoinType) {
    switch (sparkJoinType) {
      case INNER_JOIN:
        return com.google.cloud.spark.spanner.planning.relation.JoinType.INNER;
      case LEFT_OUTER_JOIN:
        return com.google.cloud.spark.spanner.planning.relation.JoinType.LEFT_OUTER;
      case RIGHT_OUTER_JOIN:
        return com.google.cloud.spark.spanner.planning.relation.JoinType.RIGHT_OUTER;
      default:
        throw new AssertionError(String.format("%s does not exist", sparkJoinType));
    }
  }
}
