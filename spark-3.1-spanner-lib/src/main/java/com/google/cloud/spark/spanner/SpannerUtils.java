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
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Partition;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.ConnectionOptions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;

public class SpannerUtils {

  public static Connection connectionFromProperties(Map<String, String> properties) {
    String connUriPrefix = "cloudspanner:";
    String emulatorHost = properties.get("emulatorHost");
    if (emulatorHost != null) {
      connUriPrefix = "cloudspanner://" + emulatorHost;
    }

    String spannerUri =
        String.format(
            connUriPrefix + "/projects/%s/instances/%s/databases/%s",
            properties.get("projectId"),
            properties.get("instanceId"),
            properties.get("databaseId"));

    ConnectionOptions.Builder builder = ConnectionOptions.newBuilder().setUri(spannerUri);
    String gcpCredsUrl = properties.get("credentials");
    if (gcpCredsUrl != null) {
      builder = builder.setCredentialsUrl(gcpCredsUrl);
    }
    ConnectionOptions opts = builder.build();
    return opts.getConnection();
  }

  public static BatchClient batchClientFromProperties(Map<String, String> properties) {
    SpannerOptions options =
        SpannerOptions.newBuilder().setProjectId(properties.get("projectId")).build();
    Spanner spanner = options.getService();
    return spanner.getBatchClient(
        DatabaseId.of(
            options.getProjectId(), properties.get("instanceId"), properties.get("databaseId")));
  }

  public static List<Row> resultSetToSparkRow(ResultSet rs) {
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

  public static Dataset<Row> datasetFromHashMap(SparkSession spark, Map<Partition, List<Row>> hm) {
    List<Row> coalescedRows = new ArrayList<Row>();
    hm.values().forEach(coalescedRows::addAll);
    Encoder<Row> rowEncoder = Encoders.bean(Row.class);
    return spark.createDataset(coalescedRows, rowEncoder);
  }
}
