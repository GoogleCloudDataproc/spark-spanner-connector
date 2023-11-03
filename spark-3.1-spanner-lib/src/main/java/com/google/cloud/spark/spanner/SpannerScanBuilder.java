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

package com.google.cloud.spark.spanner;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Allows us to implement ScanBuilder.
 */
public class SpannerScanBuilder
    implements ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns {
  private CaseInsensitiveStringMap opts;
  private Set<Filter> pushedFilters;
  private List<String> requiredColumns;
  private SpannerScanner scanner;
  private static final Logger log = LoggerFactory.getLogger(SpannerScanBuilder.class);
  private SpannerTable spannerTable;
  private Map<String, StructField> fields;

  public SpannerScanBuilder(CaseInsensitiveStringMap options) {
    this.opts = options;
    this.pushedFilters = new HashSet<Filter>();
    this.spannerTable = new SpannerTable(options);
    this.fields = new LinkedHashMap<>();
    for (StructField field : spannerTable.schema().fields()) {
      fields.put(field.name(), field);
    }
  }

  @Override
  public Scan build() {
    this.scanner =
        new SpannerScanner(this.opts.asCaseSensitiveMap(), this.spannerTable, this.fields);
    this.scanner.setFilters(this.pushedFilters());
    return this.scanner;
  }

  @Override
  public Filter[] pushedFilters() {
    return this.pushedFilters.toArray(new Filter[this.pushedFilters.size()]);
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    List<Filter> handledFilters = new ArrayList<>();
    List<Filter> unhandledFilters = new ArrayList<>();
    for (Filter filter : filters) {
      if (SparkFilterUtils.isTopLevelFieldHandled(false, filter, fields)) {
        handledFilters.add(filter);
      } else {
        unhandledFilters.add(filter);
      }
    }

    this.pushedFilters.addAll(handledFilters);
    if (this.scanner != null) {
      this.scanner.setFilters(this.pushedFilters.toArray(new Filter[this.pushedFilters.size()]));
    }
    return unhandledFilters.stream().toArray(Filter[]::new);
  }

  public StructType readSchema() {
    return this.scanner.readSchema();
  }

  /*
   * pruneColumns applies column pruning with respect to the requiredSchema.
   * The docs recommend implementing this methood to push down required columns
   * to the data source and only read these columns during scan to
   * reduce the size of the data to be read.
   */
  @Override
  public void pruneColumns(StructType requiredSchema) {
    // A user could invoke: SELECT a, b, d, a FROM TABLE;
    // and we should still be able to serve them back their
    // query without deduplication.
    List<String> requiredColumns = new ArrayList<>();
    for (StructField col : requiredSchema.fields()) {
      requiredColumns.add(col.name());
    }
    this.requiredColumns = requiredColumns;
    if (this.scanner != null) {
      this.scanner.setRequiredColumns(this.requiredColumns.toArray(new String[0]));
    }
  }
}
