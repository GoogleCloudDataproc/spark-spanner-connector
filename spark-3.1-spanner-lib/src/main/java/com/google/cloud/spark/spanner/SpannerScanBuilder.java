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

import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.PartitionOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TimestampBound;
import com.google.common.collect.Streams;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.Partition;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Allows us to implement ScanBuilder.
 */
public class SpannerScanBuilder implements Batch, ScanBuilder, SupportsPushDownFilters {
  private CaseInsensitiveStringMap opts;
  private Set<Filter> filters;
  private SpannerScanner scanner;
  private static final Logger log = LoggerFactory.getLogger(SpannerScanBuilder.class);

  public SpannerScanBuilder(CaseInsensitiveStringMap options) {
    this.opts = options;
    this.filters = new HashSet<Filter>();
  }

  @Override
  public Scan build() {
    this.scanner = new SpannerScanner(this.opts.asCaseSensitiveMap());
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
    return new SpannerPartitionReaderFactory();
  }

  @Override
  public InputPartition[] planInputPartitions() {
    // TODO: Receive the columns and filters that were pushed down.
    BatchClientWithCloser batchClient = SpannerUtils.batchClientFromProperties(this.opts);
    String sqlStmt = "SELECT * FROM " + this.opts.get("table");
    Filter[] filters = this.pushedFilters();
    if (filters.length > 0) {
      sqlStmt += " WHERE " + SparkFilterUtils.getCompiledFilter(true, filters);
    }
    try (BatchReadOnlyTransaction txn =
        batchClient.batchClient.batchReadOnlyTransaction(TimestampBound.strong())) {
      String mapAsJSON = SpannerUtils.serializeMap(this.opts.asCaseSensitiveMap());
      List<com.google.cloud.spanner.Partition> rawPartitions =
          txn.partitionQuery(
              PartitionOptions.getDefaultInstance(),
              Statement.of(sqlStmt),
              Options.dataBoostEnabled(true));

      List<Partition> parts =
          Streams.mapWithIndex(
                  rawPartitions.stream(),
                  (part, index) ->
                      new SpannerPartition(
                          part,
                          Math.toIntExact(index),
                          new SpannerInputPartitionContext(
                              part, txn.getBatchTransactionId(), mapAsJSON)))
              .collect(Collectors.toList());

      return parts.toArray(new InputPartition[0]);
    } catch (Exception e) {
      log.error("planInputPartitions exception: " + e);
      return null;
    } finally {
      batchClient.close();
    }
  }
}
