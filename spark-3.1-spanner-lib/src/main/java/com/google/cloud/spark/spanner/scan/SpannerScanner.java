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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.PartitionOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.spark.spanner.*;
import com.google.cloud.spark.spanner.planning.query.ExecutableQuery;
import com.google.common.collect.Streams;
import java.util.stream.Collectors;
import org.apache.spark.Partition;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * SpannerScanner implements Scan.
 */
public class SpannerScanner implements Batch, Scan {
  private final CaseInsensitiveStringMap opts;
  private final TimestampBound readTimestamp;
  private final StructType readSchema;
  private final ExecutableQuery executableQuery;
  private static final Logger logger = LoggerFactory.getLogger(SpannerScanner.class);

  public SpannerScanner(ExecutableQuery executableQuery) {
    this.opts = executableQuery.getOptions();
    this.readTimestamp = getReadTimestamp(this.opts);
    this.readSchema = executableQuery.getReadSchema();
    this.executableQuery = executableQuery;
    if (this.readSchema == null || this.readSchema.isEmpty()) {
      logger.info("Read Schema is null or empty");
    } else {
      logger.info("Read schema has {} fields", this.readSchema.fields().length);
      logger.info(this.readSchema.treeString());
    }
  }

  @Override
  public StructType readSchema() {
    logger.info("Reading schema from Spanner");
    return this.readSchema;
  }

  @Override
  public Batch toBatch() {
    logger.info("Reading batch from Spanner");
    return this;
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new SpannerPartitionReaderFactory();
  }

  static TimestampBound getReadTimestamp(CaseInsensitiveStringMap options) {
    String timestamp = options.get("readTimestamp");
    return TimestampBound.ofReadTimestamp(
        timestamp == null ? Timestamp.now() : Timestamp.parseTimestamp(timestamp));
  }

  @Override
  public InputPartition[] planInputPartitions() {
    logger.info("planInputPartitions");

    BatchClientWithCloser batchClient = SpannerUtils.batchClientFromProperties(this.opts);

    Statement query = executableQuery.buildStatement(batchClient.databaseClient.getDialect());

    boolean enableDataboost = false;
    if (this.opts.containsKey("enableDataBoost")) {
      enableDataboost = this.opts.get("enableDataBoost").equalsIgnoreCase("true");
    }

    logger.info("Executing PartitionQuery");
    try (BatchReadOnlyTransaction txn =
        batchClient.batchClient.batchReadOnlyTransaction(readTimestamp)) {
      String mapAsJSON = SpannerUtils.serializeMap(this.opts);
      java.util.List<com.google.cloud.spanner.Partition> rawPartitions =
          txn.partitionQuery(
              PartitionOptions.getDefaultInstance(),
              query,
              Options.dataBoostEnabled(enableDataboost));

      java.util.List<Partition> parts =
          Streams.mapWithIndex(
                  rawPartitions.stream(),
                  (part, index) ->
                      new SpannerPartition(
                          part,
                          Math.toIntExact(index),
                          new SpannerInputPartitionContext(
                              part,
                              txn.getBatchTransactionId(),
                              mapAsJSON,
                              new SpannerRowConverterDirect())))
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
