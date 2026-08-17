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

package com.google.cloud.spark.spanner.planning.query;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spark.spanner.SpannerConnectorException;
import com.google.cloud.spark.spanner.SpannerErrorCode;
import com.google.cloud.spark.spanner.SpannerUtils;
import com.google.cloud.spark.spanner.planning.relation.JoinRelation;
import com.google.cloud.spark.spanner.planning.relation.Relation;
import com.google.cloud.spark.spanner.planning.relation.TableRelation;
import com.google.cloud.spark.spanner.rendering.SpannerQueryBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** Adapts a logical table or join query for execution by the shared Spanner scanner. */
public final class LogicalQueryPlan implements ExecutableQuery {

  private final LogicalQuery logicalQuery;

  public LogicalQueryPlan(LogicalQuery logicalQuery) {
    this.logicalQuery = Objects.requireNonNull(logicalQuery, "logicalQuery");
  }

  @Override
  public CaseInsensitiveStringMap getOptions() {
    Relation source = logicalQuery.getSource();
    if (logicalQuery.sourceIsTable()) {
      return ((TableRelation) source).getTable().properties();
    }
    if (logicalQuery.sourceIsJoin()) {
      return ((TableRelation) ((JoinRelation) source).getLeft()).getTable().properties();
    }
    throw new SpannerConnectorException(
        SpannerErrorCode.UNSUPPORTED, "Source type not supported:" + source.getClass());
  }

  @Override
  public StructType getReadSchema() {
    List<String> requiredColumns = new ArrayList<>(logicalQuery.getRequiredColumnsForSchema());
    if (logicalQuery.sourceIsJoin()) {
      requiredColumns.addAll(logicalQuery.getOtherRequiredColumnsForSchema());
    }
    return SpannerUtils.pruneSchema(logicalQuery.schema(), requiredColumns);
  }

  @Override
  public Statement buildStatement(Dialect dialect) {
    CaseInsensitiveStringMap options = getOptions();
    boolean enablePredicateSql =
        options.containsKey("enablePredicateSql")
            && options.get("enablePredicateSql").equalsIgnoreCase("true");
    return SpannerQueryBuilder.newBuilder(logicalQuery, dialect, enablePredicateSql)
        .buildStatement();
  }
}
