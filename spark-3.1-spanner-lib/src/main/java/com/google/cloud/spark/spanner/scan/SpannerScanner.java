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
import com.google.cloud.spanner.*;
import com.google.cloud.spark.spanner.*;
import com.google.cloud.spark.spanner.planning.query.LogicalQuery;
import com.google.cloud.spark.spanner.planning.relation.JoinRelation;
import com.google.cloud.spark.spanner.planning.relation.Relation;
import com.google.cloud.spark.spanner.planning.relation.TableRelation;
import com.google.cloud.spark.spanner.rendering.SpannerQueryBuilder;
import com.google.common.collect.Streams;
import java.util.List;
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
  private final LogicalQuery logicalQuery;
  private static final Logger logger = LoggerFactory.getLogger(SpannerScanner.class);

  public SpannerScanner(LogicalQuery logicalQuery) {
    final Relation source = logicalQuery.getSource();
    if (logicalQuery.sourceIsTable()) {
      logger.info("logicalQuery source: TableRelation");
      this.opts = ((TableRelation) source).getTable().properties();
      logger.info("Required columns: {}", logicalQuery.getRequiredColumnsForSchema());
      this.readSchema =
          SpannerUtils.pruneSchema(
              logicalQuery.schema(), logicalQuery.getRequiredColumnsForSchema());
    } else if (logicalQuery.sourceIsJoin()) {
      logger.info("logicalQuery source: JoinRelation");
      // This assumes that a join will be between two tables and not a child join.
      this.opts = ((TableRelation) ((JoinRelation) source).getLeft()).getTable().properties();
      List<String> combinedRequiredColumns = logicalQuery.getRequiredColumnsForSchema();
      combinedRequiredColumns.addAll(logicalQuery.getOtherRequiredColumnsForSchema());
      logger.info("Combined required columns: {}", combinedRequiredColumns);
      this.readSchema = SpannerUtils.pruneSchema(logicalQuery.schema(), combinedRequiredColumns);
    } else {
      throw new SpannerConnectorException(
          SpannerErrorCode.UNSUPPORTED, "Source type not supported:" + source.getClass());
    }
    this.readTimestamp = getReadTimestamp(this.opts);
    if (this.readSchema == null || this.readSchema.isEmpty()) {
      logger.info("Read Schema is null or empty");
    } else {
      logger.info("Read schema has {} fields", this.readSchema.fields().length);
      logger.info(this.readSchema.treeString());
    }

    this.logicalQuery = logicalQuery;
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

    boolean enablePredicateSql = false;
    if (this.opts.containsKey("enablePredicateSql")) {
      enablePredicateSql = this.opts.get("enablePredicateSql").equalsIgnoreCase("true");
      logger.info("Enable Predicate Sql: {}", enablePredicateSql);
    }

    SpannerQueryBuilder result =
        SpannerQueryBuilder.newBuilder(
            this.logicalQuery, batchClient.databaseClient.getDialect(), enablePredicateSql);

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
              result.buildStatement(),
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
