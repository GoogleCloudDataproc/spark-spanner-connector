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
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LogicalQuery {
  private static final Logger logger = LoggerFactory.getLogger(LogicalQuery.class);
  private final Relation source;
  private final ImmutableList<String> requiredColumns;
  private final Filter[] pushedFilters;
  // Filters are kept separate on joins so they can be qualified with a table prefix.
  private final Filter[] pushedFiltersOther;
  private final Map<String, StructField> fields;
  private final StructType joinSchema;
  private final Map<String, ColumnResolution> resolutionMap;

  private LogicalQuery(Builder builder) {
    this.source = builder.source;
    this.requiredColumns =
        ImmutableList.copyOf(
            builder.requiredColumns != null ? builder.requiredColumns : Collections.emptyList());
    this.pushedFilters =
        builder.pushedFilters != null ? builder.pushedFilters.clone() : new Filter[0];
    this.pushedFiltersOther =
        builder.pushedFiltersOther != null ? builder.pushedFiltersOther.clone() : new Filter[0];
    this.fields = builder.fields != null ? builder.fields : Collections.emptyMap();
    this.joinSchema = builder.joinSchema != null ? builder.joinSchema : null;
    this.resolutionMap = builder.resolutionMap;
  }

  public Relation getSource() {
    return this.source;
  }

  public boolean sourceIsTable() {
    return this.source instanceof TableRelation;
  }

  public boolean sourceIsJoin() {
    return this.source instanceof JoinRelation;
  }

  public String getTableAlias() {
    if (this.sourceIsTable()) {
      return ((TableRelation) this.source).getAlias();
    } else if (this.sourceIsJoin()) {
      return ((TableRelation) ((JoinRelation) this.source).getLeft()).getAlias();
    }
    throw new SpannerConnectorException(
        SpannerErrorCode.INVALID_ARGUMENT, "LogicalQuery is not a table relation");
  }

  public String getOtherTableAlias() {
    if (this.sourceIsJoin()) {
      return ((TableRelation) ((JoinRelation) this.source).getRight()).getAlias();
    }
    throw new SpannerConnectorException(
        SpannerErrorCode.INVALID_ARGUMENT, "LogicalQuery is not a join relation");
  }

  /**
   * Required columns, ie SELECT ..., are passed to the connector via pruneColumns() for a table
   * select or pushDownJoin(leftSideRequiredColumnsWithAliases, rightSideRequiredColumnsWithAliases)
   * for a join select. requiredColumns contains table select columns. resolutionMap contains
   * columns for join select.
   *
   * @return true if Spark has indicated that the SELECT has specified specific columns.
   */
  public boolean hasRequiredColumns() {
    if (this.requiredColumns != null && !this.requiredColumns.isEmpty()) {
      return true;
    } else if (this.sourceIsJoin()) {
      for (ColumnResolution resolution : this.resolutionMap.values()) {
        if (resolution
            .getTableAlias()
            .equals(((TableRelation) ((JoinRelation) this.source).getLeft()).getTableName())) {
          return true;
        }
      }
    }
    return false;
  }

  public boolean hasOtherRequiredColumns() {
    if (this.sourceIsJoin()) {
      for (ColumnResolution resolution : this.resolutionMap.values()) {
        if (resolution
            .getTableAlias()
            .equals(((TableRelation) ((JoinRelation) this.source).getRight()).getTableName())) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * The columns in the SELECT using the alias provided by Spark, otherwise the actual column name.
   *
   * @return the aliases of the list of column names
   */
  public List<String> getRequiredColumnsForSchema() {
    return getRequiredColumns(true);
  }

  /**
   * The columns in the SELECT using the actual column name.
   *
   * @return the list of column names
   */
  public List<String> getRequiredColumnsForSelect() {
    return getRequiredColumns(false);
  }

  private List<String> getRequiredColumns(boolean asAlias) {
    if (this.sourceIsTable()) {
      return this.requiredColumns;
    } else if (this.sourceIsJoin()) {
      List<String> joinRequiredColumns = new ArrayList<>();
      this.resolutionMap.forEach(
          (key, resolution) -> {
            if (resolution
                .getTableAlias()
                .equals(((TableRelation) ((JoinRelation) this.source).getLeft()).getTableName())) {
              logger.info(
                  "getRequiredColumns. columnName: {}, asAlias: {}",
                  resolution.getColumnName(),
                  asAlias);
              joinRequiredColumns.add(
                  asAlias ? resolution.getOutputName() : resolution.getColumnName());
            }
          });
      return joinRequiredColumns;
    }
    throw new SpannerConnectorException(
        SpannerErrorCode.UNSUPPORTED, "Source type not supported:" + this.source.getClass());
  }

  /**
   * The columns for the second table in the SELECT using the alias provided by Spark, otherwise the
   * actual column name.
   *
   * @return the aliases of the list of column names
   */
  public List<String> getOtherRequiredColumnsForSchema() {
    return getOtherRequiredColumns(true);
  }

  /**
   * The columns for the second table in the SELECT using the actual column name.
   *
   * @return the list of column names
   */
  public List<String> getOtherRequiredColumnsForSelect() {
    return getOtherRequiredColumns(false);
  }

  private List<String> getOtherRequiredColumns(boolean asAlias) {
    if (this.sourceIsJoin()) {
      List<String> joinRequiredColumns = new ArrayList<>();
      this.resolutionMap.forEach(
          (key, resolution) -> {
            if (resolution
                .getTableAlias()
                .equals(((TableRelation) ((JoinRelation) this.source).getRight()).getTableName())) {
              logger.info(
                  "getOtherRequiredColumns. columnName: {}, asAlias: {}",
                  resolution.getColumnName(),
                  asAlias);
              joinRequiredColumns.add(
                  asAlias ? resolution.getOutputName() : resolution.getColumnName());
            }
          });
      return joinRequiredColumns;
    }
    throw new SpannerConnectorException(
        SpannerErrorCode.UNSUPPORTED, "Source type not supported:" + this.source.getClass());
  }

  public Filter[] getFilter() {
    return this.pushedFilters != null ? this.pushedFilters.clone() : new Filter[0];
  }

  public Filter[] getFilterOther() {
    return this.pushedFiltersOther != null ? this.pushedFiltersOther.clone() : new Filter[0];
  }

  public Map<String, StructField> getFields() {
    return this.fields;
  }

  public StructType schema() {
    if (sourceIsTable()) {
      return ((TableRelation) this.source).getTableSchema();
    }
    if (sourceIsJoin()) {
      return this.joinSchema;
    }
    throw new SpannerConnectorException(
        SpannerErrorCode.UNSUPPORTED, "Source type not supported:" + this.source.getClass());
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {

    private Relation source;
    private List<String> requiredColumns = new ArrayList<>();
    private Filter[] pushedFilters = new Filter[0];
    // Filters are kept separate on joins so they can be qualified with a table prefix.
    private Filter[] pushedFiltersOther = new Filter[0];
    private Map<String, StructField> fields = java.util.Collections.emptyMap();
    private StructType joinSchema;
    private Map<String, ColumnResolution> resolutionMap;

    private Builder() {}

    public Builder source(Relation source) {
      this.source = source;
      return this;
    }

    public Builder requiredColumns(List<String> requiredColumns) {
      this.requiredColumns = requiredColumns;
      return this;
    }

    public Builder pushedFilters(Filter[] pushedFilters) {
      this.pushedFilters = pushedFilters;
      return this;
    }

    public Builder pushedFiltersOther(Filter[] pushedFilters) {
      this.pushedFiltersOther = pushedFilters;
      return this;
    }

    public Builder fields(Map<String, StructField> fields) {
      this.fields = fields;
      return this;
    }

    public Builder joinSchema(StructType joinSchema) {
      this.joinSchema = joinSchema;
      return this;
    }

    public Builder resolutionMap(Map<String, ColumnResolution> resolutionMap) {
      this.resolutionMap = resolutionMap;
      return this;
    }

    public LogicalQuery build() {
      Objects.requireNonNull(source, "source");

      return new LogicalQuery(this);
    }
  }
}
