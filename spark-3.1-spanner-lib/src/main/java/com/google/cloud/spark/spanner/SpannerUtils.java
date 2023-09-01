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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Partition;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.Code.*;
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
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerUtils {
  public static Long MILLISECOND_TO_DAYS = 1000 * 60 * 60 * 24;
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

    return spannerStructToInternalRow(spannerRow);
  }

  private static void spannerNumericToSpark(Struct src, GenericInternalRow dest, int at) {
    // TODO: Deal with the precision truncation since Cloud Spanner's precision
    // has (precision=38, scale=9) while Apache Spark has (precision=N, scale=M)
    Decimal dec = new Decimal();
    dec.set(new scala.math.BigDecimal(src.getBigDecimal(at)), 38, 9);
    dest.setDecimal(at, dec, dec.precision());
  }

  public static InternalRow spannerStructToInternalRow(Struct spannerRow) {
    int columnCount = spannerRow.getColumnCount();
    GenericInternalRow sparkRow = new GenericInternalRow(columnCount);

    for (int i = 0; i < columnCount; i++) {
      if (spannerRow.isNull(i)) {
        sparkRow.update(i, null);
        continue;
      }

      Type typ = spannerRow.getColumnType(i);

      switch (typ.getCode()) {
        case BOOL:
          sparkRow.setBoolean(i, spannerRow.getBoolean(i));
          break;

        case DATE:
          sparkRow.update(
              i,
              ((Long)
                      (spannerRow.getDate(i).toJavaUtilDate(spannerRow.getDate(i)).getTime() / MILLISECOND_TO_DAYS))
                  .intValue());
          break;

        case FLOAT64:
          sparkRow.setDouble(i, spannerRow.getDouble(i));
          break;

        case INT64:
          sparkRow.setLong(i, spannerRow.getLong(i));
          break;

        case JSON:
          sparkRow.update(i, UTF8String.fromString(spannerRow.getJson(i)));
          break;
        case PG_JSONB:
          sparkRow.update(i, UTF8String.fromString(spannerRow.getPgJsonb(i)));
          break;

        case NUMERIC:
          spannerNumericToSpark(spannerRow, sparkRow, i);
          break;

        case PG_NUMERIC:
          spannerNumericToSpark(spannerRow, sparkRow, i);
          break;

        case TIMESTAMP:
          Timestamp timestamp = spannerRow.getTimestamp(i).toSqlTimestamp();
          // Convert the timestamp to microseconds, which is supported in the Spark.
          sparkRow.update(i, (Long) timestamp.getTime() * 1000 + timestamp.getNanos() / 1000);
          break;

        case STRING:
          sparkRow.update(i, UTF8String.fromString(spannerRow.getString(i)));
          break;

        case BYTES:
          sparkRow.update(i, new GenericArrayData(spannerRow.getBytes(i).toByteArray()));
          break;

        case STRUCT:
          sparkRow.update(i, spannerStructToInternalRow(spannerRow.getStruct(i)));
          break;

        default: // "ARRAY"
          String fieldTypeName = spannerRow.getColumnType(i).toString();
          // Note: for ARRAY<T,...>, T MUST be the homogenous (same type) within the ARRAY, per:
          // https://cloud.google.com/spanner/docs/reference/standard-sql/data-types#array_type
          if (fieldTypeName.indexOf("ARRAY<BOOL>") == 0) {
            // TODO: Update the GenericArrayData instead of using Boolean list.
            sparkRow.update(i, spannerRow.getBooleanArray(i));
          } else if (fieldTypeName.indexOf("ARRAY<STRING") == 0) {
            List<String> src = spannerRow.getStringList(i);
            List<UTF8String> dest = new ArrayList<UTF8String>(src.size());
            src.forEach((s) -> dest.add(UTF8String.fromString(s)));
            UTF8String[] utf8String = new UTF8String[dest.size()];
            sparkRow.update(i, new GenericArrayData(dest.toArray(utf8String)));
          } else if (fieldTypeName.indexOf("ARRAY<TIMESTAMP") == 0) {
            // TODO: Update the GenericArrayData instead of using ArrayList.
            List<com.google.cloud.Timestamp> tsL = spannerRow.getTimestampList(i);
            List<Timestamp> endTsL = new ArrayList<Timestamp>(tsL.size());
            tsL.forEach((ts) -> endTsL.add(ts.toSqlTimestamp()));
            sparkRow.update(i, endTsL);
          } else if (fieldTypeName.indexOf("ARRAY<DATE") == 0) {
            // TODO: Update the GenericArrayData instead of using ArrayList.
            List<Date> endDL = new ArrayList<Date>();
            spannerRow.getDateList(i).forEach((ts) -> endDL.add(ts.toJavaUtilDate(ts)));
            sparkRow.update(i, endDL);
          } else if (fieldTypeName.indexOf("ARRAY<STRUCT<") == 0) {
            // TODO: Update the GenericArrayData instead of using ArrayList.
            List<Struct> src = spannerRow.getStructList(i);
            List<InternalRow> dest = new ArrayList<>(src.size());
            src.forEach((st) -> dest.add(spannerStructToInternalRow(st)));
            sparkRow.update(i, dest);
          }
      }
    }

    return sparkRow;
  }

  public static String serializeMap(Map<String, String> m) throws JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.writer().writeValueAsString(m);
  }

  public static Map<String, String> deserializeMap(String json) throws JsonProcessingException {
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
