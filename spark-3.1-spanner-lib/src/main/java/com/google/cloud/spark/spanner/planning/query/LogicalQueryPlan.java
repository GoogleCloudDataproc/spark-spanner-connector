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
import com.google.cloud.spark.spanner.SpannerUtils;
import com.google.cloud.spark.spanner.rendering.SpannerQueryBuilder;
import com.google.cloud.spark.spanner.scan.SpannerTable;
import java.util.Objects;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** Adapts an existing logical table query for execution by the shared Spanner scanner. */
public final class LogicalQueryPlan implements ExecutableQuery {

  private final LogicalQuery logicalQuery;

  public LogicalQueryPlan(LogicalQuery logicalQuery) {
    this.logicalQuery = Objects.requireNonNull(logicalQuery, "logicalQuery");
  }

  @Override
  public CaseInsensitiveStringMap getOptions() {
    return logicalQuery.getSource().properties();
  }

  @Override
  public StructType getReadSchema() {
    SpannerTable spannerTable = logicalQuery.getSource();
    return SpannerUtils.pruneSchema(spannerTable.schema(), logicalQuery.getProjections());
  }

  @Override
  public Statement buildStatement(Dialect dialect) {
    return SpannerQueryBuilder.newBuilder(logicalQuery, dialect).buildStatement();
  }
}
