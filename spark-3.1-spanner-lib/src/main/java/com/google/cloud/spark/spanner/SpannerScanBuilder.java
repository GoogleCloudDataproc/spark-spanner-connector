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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/*
 * Allows us to implement ScanBuilder.
 */
public class SpannerScanBuilder implements ScanBuilder, SupportsPushDownFilters {
  private CaseInsensitiveStringMap opts;
  private Set<Filter> filters;
  private SpannerScanner scanner;

  public SpannerScanBuilder(CaseInsensitiveStringMap options) {
    this.opts = opts;
    this.filters = new HashSet<Filter>();
  }

  @Override
  public Scan build() {
    this.scanner = new SpannerScanner(this.opts);
    return this.scanner;
  }

  @Override
  public Filter[] pushedFilters() {
    return this.filters.toArray(new Filter[this.filters.size()]);
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    this.filters.addAll(Arrays.asList(filters));
    return this.filters.toArray(new Filter[this.filters.size()]);
  }

  public StructType readSchema() {
    return this.scanner.readSchema();
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new SpannerPartitionReaderFactory(this.opts);
  }

  /*
  * Returns a list of input partitions. Each InputPartition
  * represents a data split that can be processed by one Spark task.
  * The number of input partitions returned here is the same as the
  * number of RDD partitions this scan outputs.
  * If the Scan supports filter pushdown, this Batch is likely configured
  * with a filter and is responsible for creating splits for that
  * filter, which is not a full scan.

   This method will be called only once during a data source scan, to launch one Spark job.
  */
  @Override
  public InputPartition[] planInputPartitions() {
    // TODO: Fill me in.
    // Firstly check Cloud Spanner for the number of partitions.
    return null;
  }
}
