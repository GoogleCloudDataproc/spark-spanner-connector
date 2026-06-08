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
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.PartitionOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.spark.spanner.*;
import com.google.cloud.spark.spanner.planning.query.LogicalQuery;
import com.google.cloud.spark.spanner.planning.relation.TableRelation;
import com.google.cloud.spark.spanner.rendering.SpannerQueryBuilder;
import com.google.common.collect.Streams;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.Partition;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * SpannerScanner implements Scan.
 */
public class SpannerScanner implements Batch, Scan {
  private final SpannerTable spannerTable;
  private final Filter[] filters;
  private final Set<String> requiredColumns;
  private final CaseInsensitiveStringMap opts;
  private static final Logger log = LoggerFactory.getLogger(SpannerScanner.class);
  private final Timestamp INIT_TIME = Timestamp.now();
  private final Map<String, StructField> fields;
  private final StructType readSchema;
  private static final Logger logger = LoggerFactory.getLogger(SpannerScanner.class);

  public SpannerScanner(
      CaseInsensitiveStringMap opts,
      SpannerTable spannerTable,
      Map<String, StructField> fields,
      Filter[] filters,
      Set<String> requiredColumns) {
    this.opts = opts;
    this.spannerTable = spannerTable;
    this.fields = fields;
    this.filters = filters;
    this.requiredColumns = requiredColumns;
    this.readSchema = SpannerUtils.pruneSchema(spannerTable.schema(), requiredColumns);
  }

  @Override
  public StructType readSchema() {
    return readSchema;
  }

  @Override
  public Batch toBatch() {
    return this;
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new SpannerPartitionReaderFactory();
  }

  public static String buildColumnsWithTablePrefix(
      String tableName, Set<String> columns, boolean isPostgreSql) {
    if (tableName == null) {
      return columns.stream()
          .map(col -> isPostgreSql ? "\"" + col + "\"" : "`" + col + "`")
          .collect(Collectors.joining(", "));
    }
    String quotedTableName = isPostgreSql ? "\"" + tableName + "\"" : "`" + tableName + "`";
    return columns.stream()
        .map(col -> isPostgreSql ? "\"" + col + "\"" : "`" + col + "`")
        .map(quotedCol -> quotedTableName + "." + quotedCol)
        .collect(Collectors.joining(", "));
  }

  @Override
  public InputPartition[] planInputPartitions() {
    // Build the LogicalQuery
    ArrayList<String> columns;
    if (this.requiredColumns != null && this.requiredColumns.size() > 0) {
      columns = new ArrayList<>(this.requiredColumns);
    } else {
      columns = null;
    }
    logger.info(
        "planInputPartition columns: {} \n requiredColumns: {} \n readSchema: {} \n fields: {} \n filters: {}",
        columns,
        this.requiredColumns,
        this.readSchema,
        this.fields,
        this.filters);
    TableRelation tableRelation =
        new TableRelation(this.spannerTable.name(), this.spannerTable.name());
    LogicalQuery logicalQuery = new LogicalQuery(tableRelation, columns, Optional.empty());

    BatchClientWithCloser batchClient = SpannerUtils.batchClientFromProperties(this.opts);

    SpannerQueryBuilder result =
        SpannerQueryBuilder.newBuilder(
            logicalQuery,
            this.filters,
            this.spannerTable.schema(),
            batchClient.databaseClient.getDialect());
    String tempResults = result.buildSql();
    logger.info("tempResults: {}", tempResults);

    // End of new bits

    boolean isPostgreSql = batchClient.databaseClient.getDialect().equals(Dialect.POSTGRESQL);

    // 1. Use * if no requiredColumns were requested else select them.
    String selectPrefix = "SELECT *";
    if (this.requiredColumns != null && this.requiredColumns.size() > 0) {
      // Prefix each column with the table name to avoid ambiguity when column name
      // matches table name
      String columnsWithTablePrefix =
          buildColumnsWithTablePrefix(this.spannerTable.name(), this.requiredColumns, isPostgreSql);
      selectPrefix = "SELECT " + columnsWithTablePrefix;
    }

    String quotedTableName =
        isPostgreSql ? "\"" + spannerTable.name() + "\"" : "`" + spannerTable.name() + "`";
    String sqlStmt = selectPrefix + " FROM " + quotedTableName;
    if (this.filters.length > 0) {
      sqlStmt +=
          " WHERE "
              + SparkFilterUtils.getCompiledFilter(
                  true,
                  Optional.empty(),
                  batchClient.databaseClient.getDialect().equals(Dialect.POSTGRESQL),
                  fields,
                  this.filters);
    }
    logger.info("sqlStmt: {}", sqlStmt);

    boolean enableDataboost = false;
    if (this.opts.containsKey("enableDataBoost")) {
      enableDataboost = this.opts.get("enableDataBoost").equalsIgnoreCase("true");
    }

    try (BatchReadOnlyTransaction txn =
        batchClient.batchClient.batchReadOnlyTransaction(
            TimestampBound.ofReadTimestamp(INIT_TIME))) {
      String mapAsJSON = SpannerUtils.serializeMap(this.opts);
      java.util.List<com.google.cloud.spanner.Partition> rawPartitions =
          txn.partitionQuery(
              PartitionOptions.getDefaultInstance(),
              Statement.of(sqlStmt),
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
