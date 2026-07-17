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

import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.join.JoinType;
import org.apache.spark.sql.connector.read.SupportsPushDownJoin;
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

    if (!isInterleavedJoin(right)) {
      return false;
    }

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
}
