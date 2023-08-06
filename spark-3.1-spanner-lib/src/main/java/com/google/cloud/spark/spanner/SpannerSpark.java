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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;

/*
 * SpannerSpark is the main entry point to
 * connect Cloud Spanner with Apache Spark.
 */
public class SpannerSpark {
  private BatchClient batchClient;
  private Map<String, String> properties;

  public SpannerSpark(Map<String, String> properties) {
    SpannerOptions options = SpannerOptions.newBuilder().build();
    Spanner spanner = options.getService();
    this.batchClient =
        spanner.getBatchClient(
            DatabaseId.of(
                options.getProjectId(),
                properties.get("instanceId"),
                properties.get("databaseId")));
    this.properties = properties;
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
                List<Row> rows = resultSetToSparkRow(res);
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

  private List<Row> resultSetToSparkRow(ResultSet rs) {
    List<Row> rows = new ArrayList();
    while (rs.next()) {
      rows.add(resultSetRowToSparkRow(rs));
    }
    return rows;
  }

  public static Row resultSetRowToSparkRow(ResultSet rs) {
    Struct spannerRow = rs.getCurrentRowAsStruct();
    Integer columnCount = rs.getColumnCount();
    List<Object> objects = new ArrayList();

    for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
      String fieldTypeName = rs.getColumnType(columnIndex).toString();

      switch (fieldTypeName) {
        case "BOOL":
          objects.add(spannerRow.getBoolean(columnIndex));
          break;
        case "DATE":
          objects.add(spannerRow.getDate(columnIndex));
          break;
        case "FLOAT64":
          objects.add(spannerRow.getDouble(columnIndex));
          break;
        case "INT64":
          objects.add(spannerRow.getLong(columnIndex));
          break;
        case "JSON":
          objects.add(spannerRow.getBytes(columnIndex));
          break;
        case "NUMERIC":
          // TODO: Deal with the precision truncation since Cloud Spanner's precision
          // has (precision=38, scale=9) while Apache Spark has (precision=N, scale=M)
          objects.add(spannerRow.getBigDecimal(columnIndex));
          break;
        case "TIMESTAMP":
          objects.add(spannerRow.getTimestamp(columnIndex));
          break;
        default: // "ARRAY", "STRUCT"
          if (fieldTypeName.indexOf("BYTES") == 0) {
            objects.add(spannerRow.getBytes(columnIndex));
          } else if (fieldTypeName.indexOf("STRING") == 0) {
            objects.add(spannerRow.getString(columnIndex));
          } else if (fieldTypeName.indexOf("ARRAY<BOOL>") == 0) {
            objects.add(spannerRow.getBooleanArray(columnIndex));
          } else if (fieldTypeName.indexOf("ARRAY<STRING") == 0) {
            objects.add(spannerRow.getStringList(columnIndex));
          } else if (fieldTypeName.indexOf("ARRAY<TIMESTAMP") == 0) {
            objects.add(spannerRow.getTimestampList(columnIndex));
          } else if (fieldTypeName.indexOf("ARRAY<DATE") == 0) {
            objects.add(spannerRow.getDateList(columnIndex));
          } else if (fieldTypeName.indexOf("STRUCT") == 0) {
            // TODO: Convert into a Spark STRUCT.
          }
      }
    }

    return RowFactory.create(objects.toArray(new Object[0]));
  }
}
