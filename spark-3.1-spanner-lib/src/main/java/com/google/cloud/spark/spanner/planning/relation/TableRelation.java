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

import com.google.cloud.spark.spanner.SpannerConnectorException;
import com.google.cloud.spark.spanner.SpannerErrorCode;
import com.google.cloud.spark.spanner.scan.SpannerTable;
import java.util.Objects;
import java.util.regex.Pattern;
import org.apache.spark.sql.types.StructType;

public final class TableRelation implements Relation {
  private static final Pattern VALID_INDEX_HINT_PATTERN =
      Pattern.compile("^[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)?$");

  private final String tableName;
  private final String alias;
  private final SpannerTable table;

  public TableRelation(String tableName, String alias, SpannerTable table) {
    this.tableName = Objects.requireNonNull(tableName, "tableName cannot be null");
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

  public StructType getTableSchema() {
    return table.schema();
  }

  public String getIndexHint() {
    final String indexHint = table.properties().get("indexHint");
    if (indexHint == null) {
      return null;
    }
    String cleanHint = indexHint.trim();
    if (cleanHint.isEmpty()) {
      throw new SpannerConnectorException(SpannerErrorCode.INVALID_ARGUMENT, "Missing indexHint");
    }
    if (!VALID_INDEX_HINT_PATTERN.matcher(cleanHint).matches()) {
      throw new SpannerConnectorException(
          SpannerErrorCode.INVALID_ARGUMENT, "Invalid indexHint: " + cleanHint);
    }
    return cleanHint;
  }

  public String formatIndexHint(boolean isPostgreSql) {
    final String indexHint = getIndexHint();
    if (indexHint == null) {
      return null;
    }
    if (isPostgreSql) {
      return "/*@ FORCE_INDEX=" + indexHint + " */";
    }
    if (indexHint.contains(".")) {
      return "@{FORCE_INDEX=\"" + indexHint + "\"}";
    }
    return "@{FORCE_INDEX=" + indexHint + "}";
  }

  @Override
  public <T> T accept(RelationVisitor<T> visitor) {
    return visitor.visit(this);
  }
}
