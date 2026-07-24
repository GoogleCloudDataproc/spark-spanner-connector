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

import com.google.cloud.spark.spanner.SpannerConnectorException;
import com.google.cloud.spark.spanner.SpannerErrorCode;
import com.google.cloud.spark.spanner.planning.relation.JoinRelation;
import com.google.cloud.spark.spanner.planning.relation.Relation;
import com.google.cloud.spark.spanner.planning.relation.TableRelation;
import java.util.*;
import java.util.stream.Stream;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public final class LogicalQuery {
  private final Relation source;
  private final Set<String> requiredColumns;
  private final Filter[] pushedFilters;
  private final Map<String, StructField> fields;

  public Relation getSource() {
    return source;
  }

  public Set<String> getProjections() {
    return requiredColumns;
  }

  public Filter[] getFilter() {
    return pushedFilters != null ? pushedFilters.clone() : new Filter[0];
  }

  public Map<String, StructField> getFields() {
    return fields;
  }

  private LogicalQuery(Builder builder) {
    this.source = builder.source;
    this.requiredColumns =
        builder.requiredColumns != null
            ? builder.requiredColumns
            : java.util.Collections.emptySet();
    this.pushedFilters =
        builder.pushedFilters != null ? builder.pushedFilters.clone() : new Filter[0];
    this.fields = builder.fields != null ? builder.fields : java.util.Collections.emptyMap();
  }

  public StructType schema() {
    if (this.source instanceof TableRelation) {
      return ((TableRelation) this.source).getTableSchema();
    }
    if (this.source instanceof JoinRelation) {
      JoinRelation join = (JoinRelation) this.source;
      // Assumes that join is between two tables.
      return new StructType(
          Stream.concat(
                  Arrays.stream(((TableRelation) join.getLeft()).getTable().schema().fields()),
                  Arrays.stream(((TableRelation) join.getRight()).getTable().schema().fields()))
              .toArray(StructField[]::new));
    }
    throw new SpannerConnectorException(
        SpannerErrorCode.UNSUPPORTED, "Source type not supported:" + this.source.getClass());
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {

    private Relation source;
    private Set<String> requiredColumns = Collections.emptySet();
    private Filter[] pushedFilters = new Filter[0];
    private Map<String, StructField> fields = java.util.Collections.emptyMap();

    private Builder() {}

    public Builder source(Relation source) {
      this.source = source;
      return this;
    }

    public Builder requiredColumns(Set<String> requiredColumns) {
      this.requiredColumns = requiredColumns;
      return this;
    }

    public Builder pushedFilters(Filter[] pushedFilters) {
      this.pushedFilters = pushedFilters;
      return this;
    }

    public Builder fields(Map<String, StructField> fields) {
      this.fields = fields;
      return this;
    }

    public LogicalQuery build() {
      Objects.requireNonNull(source, "source");

      return new LogicalQuery(this);
    }
  }
}
