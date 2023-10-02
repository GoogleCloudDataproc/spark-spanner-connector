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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.PartitionOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TimestampBound;
import com.google.common.collect.Streams;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.spark.Partition;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * SpannerScanner implements Scan.
 */
public class SpannerScanner implements Batch, Scan {
  private SpannerTable spannerTable;
  private Filter[] filters;
  private String[] requiredColumns;
  private Map<String, String> opts;
  private static final Logger log = LoggerFactory.getLogger(SpannerScanner.class);
  private static final Timestamp INIT_TIME = Timestamp.now();

  public SpannerScanner(Map<String, String> opts) {
    this.opts = opts;
    this.spannerTable = new SpannerTable(opts);
  }

  @Override
  public StructType readSchema() {
    return this.spannerTable.schema();
  }

  @Override
  public Batch toBatch() {
    return this;
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new SpannerPartitionReaderFactory();
  }

  public void setFilters(Filter[] filters) {
    this.filters = filters;
  }

  public Filter[] getFilters() {
    return this.filters;
  }

  public void setRequiredColumns(String[] requiredColumns) {
    this.requiredColumns = requiredColumns;
  }

  @Override
  public InputPartition[] planInputPartitions() {
    BatchClientWithCloser batchClient = SpannerUtils.batchClientFromProperties(this.opts);

    // 1. Use * if no requiredColumns were requested else select them.
    String selectPrefix = "SELECT *";
    if (this.requiredColumns != null && this.requiredColumns.length > 0) {
      selectPrefix = "SELECT " + String.join(", ", this.requiredColumns);
    }
    String sqlStmt = selectPrefix + " FROM " + this.spannerTable.name();
    Filter[] filters = this.getFilters();
    if (filters.length > 0) {
      sqlStmt += " WHERE " + SparkFilterUtils.getCompiledFilter(true, Optional.empty(), filters);
    }

    Boolean enableDataboost = this.opts.get("enableDataboost") == "true";

    try (BatchReadOnlyTransaction txn =
        batchClient.batchClient.batchReadOnlyTransaction(
            TimestampBound.ofReadTimestamp(INIT_TIME))) {
      String mapAsJSON = SpannerUtils.serializeMap(this.opts);
      List<com.google.cloud.spanner.Partition> rawPartitions =
          txn.partitionQuery(
              PartitionOptions.getDefaultInstance(),
              Statement.of(sqlStmt),
              Options.dataBoostEnabled(enableDataboost));

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
    } catch (JsonProcessingException e) {
      throw new SpannerConnectorException(
          SpannerErrorCode.SPANNER_FAILED_TO_PARSE_OPTIONS, "Error parsing the input options.", e);
    } finally {
      batchClient.close();
    }
  }
}
