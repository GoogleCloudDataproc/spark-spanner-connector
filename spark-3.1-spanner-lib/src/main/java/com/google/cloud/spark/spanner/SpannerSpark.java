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

import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.Partition;
import com.google.cloud.spanner.PartitionOptions;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.ConnectionOptions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class SpannerSpark implements DataSourceRegister, TableProvider {
  private BatchClient batchClient;

  public SpannerSpark(Map<String, String> properties) {
    SpannerOptions options = SpannerOptions.newBuilder().build();
    Spanner spanner = options.getService();
    this.batchClient =
        spanner.getBatchClient(
            DatabaseId.of(
                options.getProjectId(),
                properties.get("instanceId"),
                properties.get("databaseId")));
  }

  public Dataset<Row> execute(SparkSession spark, String sqlStmt) {
    // 1. TODO: Verify that sqlStmt is ONLY Data-Query-Language and not DML nor DDL.

    // 2. Spin up the parallelizing executor.
    ExecutorService executor =
        Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    try (final BatchReadOnlyTransaction txn =
        this.batchClient.batchReadOnlyTransaction(TimestampBound.strong())) {
      List<Partition> partitions =
          txn.partitionQuery(
              PartitionOptions.getDefaultInstance(),
              Statement.of(sqlStmt),
              Options.dataBoostEnabled(true));

      ConcurrentHashMap hm = new ConcurrentHashMap(partitions.size());
      for (final Partition partition : partitions) {
        executor.execute(
            () -> {
              // Run partitions in parallel, then combine them.
              try (ResultSet res = txn.execute(partition)) {
                // Create rows per
                List<Row> rows = ResultSetToSparkRow(res);
                hm.put(partition, rows);
              }
            });
      }
      return datasetFromHashMap(spark, hm);
    } finally {
      executor.shutdown();
      try {
        executor.awaitTermination(10, TimeUnit.MINUTES);
      } catch (Exception e) {
        // TODO: Log this error properly.
      }
    }
  }

  private Dataset<Row> datasetFromHashMap(SparkSession spark, Map<Partition, List<Row>> hm) {
    List<Row> coalescedRows = new ArrayList<Row>();
    hm.values().forEach(coalescedRows::addAll);
    Encoder<Row> rowEncoder = Encoders.bean(Row.class);
    return spark.createDataset(coalescedRows, rowEncoder);
  }

  private List<Row> ResultSetToSparkRow(ResultSet rs) {
    List<Row> rows = new ArrayList();
    while (rs.next()) {
      rows.add(ResultSetIndexToRow(rs));
    }
    return rows;
  }

  private Row ResultSetIndexToRow(ResultSet rs) {
    Struct spannerRow = rs.getCurrentRowAsStruct();
    Integer columnCount = rs.getColumnCount();
    List<Object> objects = new ArrayList();

    for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
      String fieldTypeName = rs.getColumnType(columnIndex).toString();
      int openBracIndex = StringUtils.indexOf(fieldTypeName, '(');
      if (openBracIndex >= 0) {
        fieldTypeName = StringUtils.truncate(fieldTypeName, openBracIndex);
      }

      switch (fieldTypeName) {
        case "BOOL":
          objects.add(spannerRow.getBoolean(columnIndex));
        case "BYTES":
          objects.add(spannerRow.getBytes(columnIndex));
        case "DATE":
          objects.add(spannerRow.getDate(columnIndex));
        case "FLOAT64":
          objects.add(spannerRow.getBigDecimal(columnIndex));
        case "INT64":
          objects.add(spannerRow.getLong(columnIndex));
        case "JSON":
          objects.add(spannerRow.getBytes(columnIndex));
        case "NUMERIC":
          objects.add(spannerRow.getBigDecimal(columnIndex));
        case "STRING":
          objects.add(spannerRow.getString(columnIndex));
        case "TIMESTAMP":
          objects.add(spannerRow.getTimestamp(columnIndex));
        default: // "ARRAY", "STRUCT"
          // throw new Exception("unhandled type: " + fieldTypeName);
      }
    }

    return RowFactory.create(objects.toArray(new Object[0]));
  }

  @Override
  public String shortName() {
    return "cloud-spanner";
  }

  @Override
  public Transform[] inferPartitioning(CaseInsensitiveStringMap options) {
    return null;
  }

  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    return null;
  }

  @Override
  public boolean supportsExternalMetadata() {
    return false;
  }

  @Override
  public Table getTable(
      StructType schema, Transform[] partitioning, Map<String, String> properties) {
    // 1. Create the DatabaseClient with the provided options:
    // "instanceId": <INSTANCE_ID>
    // "projectId": <PROJECT_ID>
    // "databaseId": <DATABASE_ID>
    String spannerUri =
        String.format(
            "cloudspanner:/projects/%s/instances/%s/databases/%s",
            properties.get("projectId"),
            properties.get("instanceId"),
            properties.get("databaseId"));

    ConnectionOptions.Builder builder = ConnectionOptions.newBuilder().setUri(spannerUri);
    String gcpCredsUrl = properties.get("credentials");
    if (gcpCredsUrl != null) {
      builder = builder.setCredentialsUrl(gcpCredsUrl);
    }
    ConnectionOptions opts = builder.build();

    try (Connection conn = opts.getConnection()) {
      String tableName = properties.get("table");
      // 3. Run an information schema query to get the type definition of the table.
      Statement stmt =
          Statement.newBuilder(
                  "SELECT COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE, SPANNER_TYPE "
                      + "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=@tableName "
                      + "ORDER BY ORDINAL_POSITION")
              .bind("tableName")
              .to(tableName)
              .build();
      try (final ResultSet rs = conn.executeQuery(stmt)) {
        return new SpannerTable(tableName, rs);
      }
    }
  }
}