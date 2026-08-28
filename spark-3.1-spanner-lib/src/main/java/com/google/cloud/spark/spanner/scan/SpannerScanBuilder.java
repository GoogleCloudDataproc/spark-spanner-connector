// Copyright 2023 Google LLC
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

import com.google.cloud.spark.spanner.SpannerConnectorException;
import com.google.cloud.spark.spanner.SpannerErrorCode;
import com.google.cloud.spark.spanner.SparkFilterUtils;
import com.google.cloud.spark.spanner.planning.query.ColumnResolution;
import com.google.cloud.spark.spanner.planning.query.LogicalQuery;
import com.google.cloud.spark.spanner.planning.query.LogicalQueryPlan;
import com.google.cloud.spark.spanner.planning.relation.JoinRelation;
import com.google.cloud.spark.spanner.planning.relation.TableRelation;
import com.google.common.collect.ImmutableList;
import java.util.*;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Allows us to implement ScanBuilder.
 */
public class SpannerScanBuilder
    implements ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns {
  private List<Filter> pushedFilters;
  private ImmutableList<String> requiredColumns;
  private SpannerScanner scanner;
  private static final Logger logger = LoggerFactory.getLogger(SpannerScanBuilder.class);
  private SpannerTable spannerTable;
  private Map<String, StructField> fields;
  private JoinRelation join;
  private StructType joinSchema;
  private Map<String, ColumnResolution> resolutionMap;
  private SpannerScanBuilder right = null;

  public SpannerScanBuilder(SpannerTable spannerTable) {
    logger.info("this={} {}", System.identityHashCode(this), spannerTable.name());
    this.pushedFilters = new ArrayList<>();
    this.spannerTable = spannerTable;
    this.fields = new LinkedHashMap<>();
    for (StructField field : this.spannerTable.schema().fields()) {
      this.fields.put(field.name(), field);
    }
  }

  @Override
  public Scan build() {
    logger.info("build() this={}", System.identityHashCode(this));
    // Build the LogicalQuery
    LogicalQuery.Builder builder = LogicalQuery.builder();

    if (this.join != null) {
      logger.info("building join");
      builder.source(this.join).joinSchema(this.joinSchema).resolutionMap(this.resolutionMap);
      // Assume that in a join this will be between two tables. Combine schema of tables.
      for (StructField field :
          ((TableRelation) this.join.getRight()).getTable().schema().fields()) {
        this.fields.put(field.name(), field);
      }
    } else if (this.spannerTable != null) {
      logger.info("building spanner table");
      builder.source(createTableRelation());
    } else {
      throw new SpannerConnectorException(SpannerErrorCode.UNSUPPORTED, "Source type missing");
    }

    builder.requiredColumns(this.requiredColumns).fields(this.fields);

    logger.info("building logicalQuery with filters this={}", System.identityHashCode(this));
    if (this.right != null) {
      builder.pushedFiltersOther(right.pushedFilters());
    }
    builder.pushedFilters(this.pushedFilters());

    final LogicalQuery logicalQuery = builder.build();

    logger.info(
        "build() this={}, logicalQuery={}",
        System.identityHashCode(this),
        System.identityHashCode(logicalQuery));

    this.scanner = new SpannerScanner(new LogicalQueryPlan(logicalQuery));
    logger.debug("build() {}", this.scanner.readSchema().treeString());
    logger.info("build() {}", logicalQuery.getSource());
    logger.debug("Required columns passed to LogicalQuery:");
    if (this.requiredColumns != null) {
      this.requiredColumns.forEach(c -> logger.debug("  {}", c));
    }

    return this.scanner;
  }

  @Override
  public Filter[] pushedFilters() {
    Filter[] filters = this.pushedFilters.toArray(new Filter[0]);
    logger.info(
        "pushed filters: this={}, {}",
        System.identityHashCode(this),
        SparkFilterUtils.filtersToString(filters));
    return filters;
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    logger.info(
        "push filters: this={}, {}",
        System.identityHashCode(this),
        SparkFilterUtils.filtersToString(filters));
    List<Filter> handledFilters = new ArrayList<>();
    List<Filter> unhandledFilters = new ArrayList<>();
    for (Filter filter : filters) {
      if (SparkFilterUtils.isTopLevelFieldHandled(false, filter, this.fields)) {
        handledFilters.add(filter);
      } else {
        unhandledFilters.add(filter);
      }
    }
    this.pushedFilters.addAll(handledFilters);
    return unhandledFilters.stream().toArray(Filter[]::new);
  }

  /*
   * pruneColumns applies column pruning with respect to the requiredSchema.
   * The docs recommend implementing this method to push down required columns
   * to the data source and only read these columns during scan to
   * reduce the size of the data to be read.
   */
  @Override
  public void pruneColumns(StructType requiredSchema) {
    // A user could invoke: SELECT a, b, d, a FROM TABLE;
    // and we should still be able to serve them back their
    // query without deduplication.
    logger.info("pruning columns. requiredSchema: {}", requiredSchema);
    this.requiredColumns = ImmutableList.copyOf(requiredSchema.fieldNames());
  }

  public void setJoin(
      JoinRelation join,
      StructType joinSchema,
      Map<String, ColumnResolution> resolutionMap,
      SpannerScanBuilder right) {
    logger.info("setJoin: {}", join);
    this.join = join;
    this.joinSchema = joinSchema;
    this.resolutionMap = resolutionMap;
    this.right = right;
  }

  public String getDatabaseId() {
    return spannerTable.getDatabaseId();
  }

  public String getInstanceId() {
    return spannerTable.getInstanceId();
  }

  public String getProjectId() {
    return spannerTable.getProjectId();
  }

  public StructType getSchema() {
    logger.info("getSchema");
    return spannerTable.schema();
  }

  public String getTableName() {
    logger.info("getTableName");
    return spannerTable.name();
  }

  public Map<String, String> getSpannerTypes() {
    return spannerTable.getSpannerTypes();
  }

  public TableRelation createTableRelation() {
    // Join pushdown currently supports only interleaved parent/child tables.
    // Self-joins are rejected by isOtherSideCompatibleForJoin(), so using the
    // table name as the default alias is currently safe.
    logger.info("createTableRelation {}", spannerTable.name());
    return new TableRelation(this.spannerTable.name(), this.spannerTable.name(), this.spannerTable);
  }

  public InterleaveTableMetadata getInterleavedTableMetadata() {
    logger.info("getInterleavedTableMetadata");
    return spannerTable.getInterleavedTableMetadata();
  }
}
