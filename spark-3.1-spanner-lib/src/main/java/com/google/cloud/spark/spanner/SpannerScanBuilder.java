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
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/*
 * Allows us to implement ScanBuilder.
 */
public class SpannerScanBuilder implements Batch, ScanBuilder, SupportsPushDownFilters {
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

  public Batch toBatch() {
    return this;
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

  @Override
  public InputPartition[] planInputPartitions() {
    // TODO: Fill me in.
    return null;
  }
}
