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

import com.google.cloud.spark.spanner.scan.SpannerTable;
import java.util.Objects;

public final class TableRelation implements Relation {
  private final String tableName;
  private final String alias;
  private final SpannerTable table;

  public TableRelation(String tableName, String alias, SpannerTable table) {
    this.tableName = tableName;
    this.alias = alias;
    this.table = Objects.requireNonNull(table, "table cannot be null");
  }

  public String getAlias() {
    return alias;
  }

  public String getTableName() {
    return tableName;
  }

  public SpannerTable getTable() {
    return table;
  }

  @Override
  public <T> T accept(RelationVisitor<T> visitor) {
    return visitor.visit(this);
  }
}
