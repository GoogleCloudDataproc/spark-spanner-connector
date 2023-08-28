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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Partition;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.ConnectionOptions;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

public class SpannerUtils {
  public static Map<String, String> defaultConnOpts =
      new HashMap<String, String>() {
        {
          put("projectId", "orijtech-161805");
          put("instanceId", "spanner-spark");
          put("databaseId", "spark-db");
          put("enableDataBoost", "true");
          put("table", "games");
        }
      };

  public static Connection connectionFromProperties(Map<String, String> properties) {
    if (properties == null || properties.size() == 0) {
      properties = defaultConnOpts;
    }
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

  public static BatchClientWithCloser batchClientFromProperties(Map<String, String> properties) {
    if (properties == null) {
      properties = defaultConnOpts;
    }
    SpannerOptions options =
        SpannerOptions.newBuilder().setProjectId(properties.get("projectId")).build();
    Spanner spanner = options.getService();
    return new BatchClientWithCloser(
        spanner,
        spanner.getBatchClient(
            DatabaseId.of(
                options.getProjectId(),
                properties.get("instanceId"),
                properties.get("databaseId"))));
  }

  public static List<InternalRow> resultSetToSparkRow(ResultSet rs) {
    List<InternalRow> rows = new ArrayList<>();
    while (rs.next()) {
      rows.add(resultSetRowToInternalRow(rs));
    }
    return rows;
  }

  public static InternalRow resultSetRowToInternalRow(ResultSet rs) {
    Struct spannerRow = rs.getCurrentRowAsStruct();
    Integer columnCount = rs.getColumnCount();

    return spannerStructToInternalRow(rs, spannerRow, columnCount);
  }

  public static InternalRow spannerStructToInternalRow(
      ResultSet rs, Struct spannerRow, int columnCount) {
    GenericInternalRow sparkRow = new GenericInternalRow(columnCount);

    for (int i = 0; i < columnCount; i++) {
      // Using a string typename to aid in easy traversal in the meantime
      // of non-composite ARRAY<T> kinds. TODO: Traverse ARRAY and STRUCT
      // programmatically by access of the type elements.
      String fieldTypeName = rs.getColumnType(i).toString();

      if (spannerRow.isNull(i)) {
        // TODO: Examine if we should perhaps translate to
        // zero values for simple types like INT64, FLOAT64
        // or just keep it as null.
        sparkRow.update(i, null);
        continue;
      }

      switch (fieldTypeName) {
        case "BOOL":
          sparkRow.setBoolean(i, spannerRow.getBoolean(i));
          break;

        case "DATE":
          sparkRow.update(i, spannerRow.getDate(i).toJavaUtilDate(spannerRow.getDate(i)));
          break;

        case "FLOAT64":
          sparkRow.setDouble(i, spannerRow.getDouble(i));
          break;

        case "INT64":
          sparkRow.setLong(i, spannerRow.getLong(i));
          break;

        case "JSON":
          sparkRow.update(i, UTF8String.fromString(spannerRow.getString(i)));
          break;

        case "NUMERIC":
          // TODO: Deal with the precision truncation since Cloud Spanner's precision
          // has (precision=38, scale=9) while Apache Spark has (precision=N, scale=M)
          Decimal dec = new Decimal();
          dec.set(new scala.math.BigDecimal(spannerRow.getBigDecimal(i)), 38, 9);
          sparkRow.setDecimal(i, dec, dec.precision());
          break;

        case "TIMESTAMP":
          sparkRow.update(i, spannerRow.getTimestamp(i).toSqlTimestamp());
          break;

        default: // "ARRAY", "STRUCT"
          if (fieldTypeName.indexOf("BYTES") == 0) {
            sparkRow.update(i, spannerRow.getBytes(i).toByteArray());
          } else if (fieldTypeName.indexOf("STRING") == 0) {
            sparkRow.update(i, UTF8String.fromString(spannerRow.getString(i)));
          } else if (fieldTypeName.indexOf("ARRAY<BOOL>") == 0) {
            sparkRow.update(i, spannerRow.getBooleanArray(i));
          } else if (fieldTypeName.indexOf("ARRAY<STRING") == 0) {
            List<String> src = spannerRow.getStringList(i);
            List<UTF8String> dest = new ArrayList<UTF8String>(src.size());
            src.forEach((s) -> dest.add(UTF8String.fromString(s)));
            sparkRow.update(i, dest);
          } else if (fieldTypeName.indexOf("ARRAY<TIMESTAMP") == 0) {
            List<com.google.cloud.Timestamp> tsL = spannerRow.getTimestampList(i);
            List<Timestamp> endTsL = new ArrayList<Timestamp>(tsL.size());
            tsL.forEach((ts) -> endTsL.add(ts.toSqlTimestamp()));
            sparkRow.update(i, endTsL);
          } else if (fieldTypeName.indexOf("ARRAY<DATE") == 0) {
            List<Date> endDL = new ArrayList<Date>();
            spannerRow.getDateList(i).forEach((ts) -> endDL.add(ts.toJavaUtilDate(ts)));
            sparkRow.update(i, endDL);
          } else if (fieldTypeName.indexOf("STRUCT") == 0) {
            if (spannerRow.isNull(i)) {
              sparkRow.update(i, null);
            } else {
              // TODO: Convert into a Spark STRUCT.
            }
          }
      }
    }

    return sparkRow;
  }

  public static String serializeMap(Map<String, String> m) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.writer().writeValueAsString(m);
  }

  public static Map<String, String> deserializeMap(String json) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    TypeReference<HashMap<String, String>> typeRef =
        new TypeReference<HashMap<String, String>>() {};
    return mapper.readValue(json, typeRef);
  }

  public static Dataset<Row> datasetFromHashMap(SparkSession spark, Map<Partition, List<Row>> hm) {
    List<Row> coalescedRows = new ArrayList<Row>();
    hm.values().forEach(coalescedRows::addAll);
    Encoder<Row> rowEncoder = Encoders.bean(Row.class);
    return spark.createDataset(coalescedRows, rowEncoder);
  }
}
