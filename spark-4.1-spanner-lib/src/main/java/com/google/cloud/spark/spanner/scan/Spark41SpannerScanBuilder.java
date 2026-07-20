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
import com.google.cloud.spark.spanner.planning.query.PredicateToExprConverter;
import com.google.cloud.spark.spanner.planning.relation.JoinRelation;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.join.JoinType;
import org.apache.spark.sql.connector.read.SupportsPushDownJoin;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Spark41SpannerScanBuilder extends SpannerScanBuilder implements SupportsPushDownJoin {
  private static final Logger logger = LoggerFactory.getLogger(Spark41SpannerScanBuilder.class);

  public Spark41SpannerScanBuilder(SpannerTable spannerTable) {
    super(spannerTable);
  }

  public boolean isOtherSideCompatibleForJoin(SupportsPushDownJoin other) {
    if (!(other instanceof SpannerScanBuilder)) {
      return false;
    }

    SpannerScanBuilder otherScan = (SpannerScanBuilder) other;

    return this.getDatabaseId().equals(otherScan.getDatabaseId())
        && this.getInstanceId().equals(otherScan.getInstanceId());
  }

  public boolean pushDownJoin(
      SupportsPushDownJoin other,
      JoinType joinType,
      ColumnWithAlias[] leftSideRequiredColumnsWithAliases,
      ColumnWithAlias[] rightSideRequiredColumnsWithAliases,
      Predicate condition) {
    if (!(other instanceof SpannerScanBuilder)) {
      return false;
    }
    SpannerScanBuilder right = (SpannerScanBuilder) other;

    if (!isJoinTypeAllowed(joinType)) {
      return false;
    }

    if (!isInterleavedJoin(right)) {
      return false;
    }

    // Combine the schema of the left and right required columns.
    // This is the schema of the ON clause.
    StructType joinSchema =
        calculateJoinOutputSchema(leftSideRequiredColumnsWithAliases, this.getSchema());

    joinSchema =
        joinSchema.merge(
            calculateJoinOutputSchema(rightSideRequiredColumnsWithAliases, right.getSchema()),
            false);

    BoolExpr predicate = PredicateToExprConverter.translatePredicate(condition, joinSchema);
    JoinRelation joinRelation =
        new JoinRelation(
            this.getTableRelation(),
            right.getTableRelation(),
            sparkToConnector(joinType),
            predicate);
    setJoin(joinRelation);

    return true;
  }

  private boolean isInterleavedJoin(SpannerScanBuilder other) {
    final InterleaveTableMetadata thisTableMetadata = this.getInterleavedTableMetadata();
    final InterleaveTableMetadata otherTableMetadata = other.getInterleavedTableMetadata();
    final String thisTableParent = thisTableMetadata.getTableName();
    final String otherTableParent = otherTableMetadata.getTableName();
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

      StructField field = schema.apply(columnName);

      if (alias == null) {
        newSchema = newSchema.add(field);
      } else {
        newSchema = newSchema.add(alias, field.dataType(), field.nullable(), field.metadata());
      }
    }

    return newSchema;
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
