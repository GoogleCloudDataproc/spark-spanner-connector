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

public class SpannerUtils {
  private static final ObjectMapper jsonMapper = new ObjectMapper();

  public static Long MILLISECOND_TO_DAYS = 1000 * 60 * 60 * 24L;

  public static Connection connectionFromProperties(Map<String, String> properties) {
    String connUriPrefix = "cloudspanner:";
    String emulatorHost = properties.get("emulatorHost");
    if (emulatorHost != null) {
      connUriPrefix = "cloudspanner://" + emulatorHost;
    }

    String spannerUri =
        String.format(
            connUriPrefix
                + "/projects/%s/instances/%s/databases/%s?autoConfigEmulator=%s;usePlainText=%s",
            properties.get("projectId"),
            properties.get("instanceId"),
            properties.get("databaseId"),
            emulatorHost != null,
            emulatorHost != null);

    ConnectionOptions.Builder builder = ConnectionOptions.newBuilder().setUri(spannerUri);
    ConnectionOptions opts = builder.build();
    return opts.getConnection();
  }

  public static BatchClientWithCloser batchClientFromProperties(Map<String, String> properties) {
    SpannerOptions.Builder builder =
        SpannerOptions.newBuilder().setProjectId(properties.get("projectId"));

    String emulatorHost = properties.get("emulatorHost");
    if (emulatorHost != null) {
      builder = builder.setEmulatorHost(emulatorHost);
    }

    SpannerOptions options = builder.build();
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

  public static void asSparkDecimal(GenericInternalRow dest, java.math.BigDecimal v, int at) {
    // TODO: Deal with the precision truncation since Cloud Spanner's precision
    // has (precision=38, scale=9) while Apache Spark has (precision=N, scale=M)
    Decimal dec = new Decimal();
    dec.set(new scala.math.BigDecimal(v), 38, 9);
    dest.setDecimal(at, dec, dec.precision());
  }

  private static void spannerNumericToSpark(Struct src, GenericInternalRow dest, int at) {
    asSparkDecimal(dest, src.getBigDecimal(at), at);
  }

  public static Long timestampToLong(Timestamp ts) {
    // Convert the timestamp to microseconds, which is supported in the Spark.
    return (ts.getTime() * 1000 + ts.getNanos()) / 1000;
  }

  public static Integer dateToInteger(Date d) {
    return ((Long) (d.getTime() / MILLISECOND_TO_DAYS)).intValue();
  }

  public static GenericArrayData timestampIterToSpark(Iterable<Timestamp> tsIt) {
    List<Long> dest = new ArrayList<>();
    tsIt.forEach((ts) -> dest.add(timestampToLong(ts)));
    return new GenericArrayData(dest.toArray(new Long[0]));
  }

  public static GenericArrayData dateIterToSpark(Iterable<Date> tsIt) {
    List<Integer> dest = new ArrayList<>();
    tsIt.forEach((ts) -> dest.add(dateToInteger(ts)));
    return new GenericArrayData(dest.toArray(new Integer[0]));
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
          Date date = spannerRow.getDate(i).toJavaUtilDate(spannerRow.getDate(i));
          sparkRow.update(i, dateToInteger(date));
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
          sparkRow.update(i, timestampToLong(timestamp));
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
            sparkRow.update(i, new GenericArrayData(spannerRow.getBooleanArray(i)));
          } else if (fieldTypeName.indexOf("ARRAY<FLOAT64>") == 0) {
            sparkRow.update(i, new GenericArrayData(spannerRow.getDoubleArray(i)));
          } else if (fieldTypeName.indexOf("ARRAY<INT64>") == 0) {
            sparkRow.update(i, new GenericArrayData(spannerRow.getLongArray(i)));
          } else if (fieldTypeName.indexOf("ARRAY<STRING") == 0) {
            List<String> src = spannerRow.getStringList(i);
            List<UTF8String> dest = new ArrayList<UTF8String>(src.size());
            src.forEach((s) -> dest.add(UTF8String.fromString(s)));
            sparkRow.update(i, new GenericArrayData(dest.toArray(new UTF8String[0])));
          } else if (fieldTypeName.indexOf("ARRAY<TIMESTAMP") == 0) {
            List<com.google.cloud.Timestamp> tsL = spannerRow.getTimestampList(i);
            List<Long> endTsL = new ArrayList<>();
            tsL.forEach((ts) -> endTsL.add(timestampToLong(ts.toSqlTimestamp())));
            sparkRow.update(i, new GenericArrayData(endTsL.toArray(new Long[0])));
          } else if (fieldTypeName.indexOf("ARRAY<DATE>") == 0) {
            List<Integer> endDL = new ArrayList<>();
            spannerRow
                .getDateList(i)
                .forEach((ts) -> endDL.add(dateToInteger(ts.toJavaUtilDate(ts))));
            sparkRow.update(i, new GenericArrayData(endDL.toArray(new Integer[0])));
          } else if (fieldTypeName.indexOf("ARRAY<STRUCT<") == 0) {
            List<Struct> src = spannerRow.getStructList(i);
            List<InternalRow> dest = new ArrayList<>(src.size());
            src.forEach((st) -> dest.add(spannerStructToInternalRow(st)));
            sparkRow.update(i, new GenericArrayData(dest));
          } else {
            sparkRow.update(i, null);
          }
      }
    }

    return sparkRow;
  }

  public static String serializeMap(Map<String, String> m) throws JsonProcessingException {
    return jsonMapper.writer().writeValueAsString(m);
  }

  public static Map<String, String> deserializeMap(String json) throws JsonProcessingException {
    TypeReference<HashMap<String, String>> typeRef =
        new TypeReference<HashMap<String, String>>() {};
    return jsonMapper.readValue(json, typeRef);
  }

  public static Dataset<Row> datasetFromHashMap(SparkSession spark, Map<Partition, List<Row>> hm) {
    List<Row> coalescedRows = new ArrayList<Row>();
    hm.values().forEach(coalescedRows::addAll);
    Encoder<Row> rowEncoder = Encoders.bean(Row.class);
    return spark.createDataset(coalescedRows, rowEncoder);
  }
}
