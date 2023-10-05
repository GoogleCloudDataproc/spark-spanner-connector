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
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.cloud.ByteArray;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Partition;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.Code.*;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.ConnectionOptions;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
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
import org.threeten.bp.Duration;

public class SpannerUtils {
  private static final RetrySettings RETRY_SETTING =
      RetrySettings.newBuilder()
          .setInitialRetryDelay(Duration.ofMillis(500))
          .setMaxRetryDelay(Duration.ofSeconds(16))
          .setRetryDelayMultiplier(1.5)
          .setMaxAttempts(5)
          .build();
  private static final ObjectMapper jsonMapper = new ObjectMapper();

  public static Long SECOND_TO_DAYS = 60 * 60 * 24L;

  // TODO: Infer the UserAgent's version from the library version dynamically.
  private static String USER_AGENT = "spark-spanner/v0.0.1";

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
    // TODO: Fix out how to add the gRPC UserAgent when creating this Connection.
    ConnectionOptions opts = builder.build();
    return opts.getConnection();
  }

  public static BatchClientWithCloser batchClientFromProperties(Map<String, String> properties) {
    SpannerOptions.Builder builder =
        SpannerOptions.newBuilder()
            .setProjectId(properties.get("projectId"))
            .setHeaderProvider(FixedHeaderProvider.create("user-agent", USER_AGENT));
    builder
        .getSpannerStubSettingsBuilder()
        .executeSqlSettings()
        .setRetryableCodes(
            Code.UNAVAILABLE, Code.RESOURCE_EXHAUSTED, Code.INTERNAL, Code.DEADLINE_EXCEEDED)
        .setRetrySettings(RETRY_SETTING);

    builder
        .getSpannerStubSettingsBuilder()
        .partitionQuerySettings()
        .setRetryableCodes(
            Code.UNAVAILABLE, Code.RESOURCE_EXHAUSTED, Code.INTERNAL, Code.DEADLINE_EXCEEDED)
        .setRetrySettings(RETRY_SETTING);

    builder
        .getSpannerStubSettingsBuilder()
        .readSettings()
        .setRetryableCodes(
            Code.UNAVAILABLE, Code.RESOURCE_EXHAUSTED, Code.INTERNAL, Code.DEADLINE_EXCEEDED)
        .setRetrySettings(RETRY_SETTING);

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

  public static void toSparkDecimal(GenericInternalRow dest, java.math.BigDecimal v, int at) {
    Decimal dec = asSparkDecimal(v);
    dest.setDecimal(at, dec, dec.precision());
  }

  private static Decimal asSparkDecimal(java.math.BigDecimal v) {
    if (v == null) {
      return null;
    }
    // TODO: Deal with the precision truncation since Cloud Spanner's precision
    // has (precision=38, scale=9) while Apache Spark has (precision=N, scale=M)
    Decimal dec = new Decimal();
    dec.set(new scala.math.BigDecimal(v), 38, 9);
    return dec;
  }

  private static void spannerNumericToSpark(Struct src, GenericInternalRow dest, int at) {
    toSparkDecimal(dest, src.getBigDecimal(at), at);
  }

  public static Long toSparkTimestamp(com.google.cloud.Timestamp ts) {
    if (ts == null) {
      return null;
    }
    // Convert the timestamp to microseconds, which is supported in the Spark.
    Timestamp sqlTs = ts.toSqlTimestamp();
    return toSparkTimestamp(sqlTs);
  }

  /*
   * toSparkTimestamp converts a java.sql.Timestamp to microseconds,
   * stored in a Long as Spark expects.
   */
  public static Long toSparkTimestamp(Timestamp ts) {
    if (ts == null) {
      return null;
    }
    // ts.getTime() returns time in milliseconds, so *1000 -> microseconds
    // ts.getNanos() returns time in nanoseconds, so /1000 -> microseconds
    return (ts.getTime() * 1000) + (ts.getNanos() / 1000);
  }

  public static Long zonedDateTimeToSparkTimestamp(ZonedDateTime zdt) {
    // Convert the zonedDateTime to microseconds which Spark supports.
    return zdt.toEpochSecond() * 1_000_000;
  }

  /*
   * zonedDateTimeToSparkDate converts a ZonedDateTime to number of days
   * since the Epoch: January 1st 1970, which is what Spark understands.
   */
  public static Integer zonedDateTimeToSparkDate(ZonedDateTime zdt) {
    return ((Long) (zdt.toEpochSecond() / SECOND_TO_DAYS)).intValue();
  }

  private static ZoneId zoneUTC = ZoneId.of("UTC+00:00");

  /*
   * toSparkDate converts a Google Date into the number of Days since
   * the Epoch: January 1st 1970, which is what Spark understands.
   */
  public static Integer toSparkDate(com.google.cloud.Date dc) {
    if (dc == null) {
      return null;
    }
    // Cloud Spanner doesn't attach a zone to the Date, returning the time in UTC
    // so we can't let it be interpreted in that of the system, hence using ZonedTimeDate with UTC.
    // and not converting it to JavaUtilDate which uses the local system's timezone.
    ZonedDateTime zdt =
        ZonedDateTime.of(dc.getYear(), dc.getMonth(), dc.getDayOfMonth(), 0, 0, 0, 0, zoneUTC);
    return zonedDateTimeToSparkDate(zdt);
  }

  public static GenericArrayData timestampIterToSpark(Iterable<Timestamp> tsIt) {
    List<Long> dest = new ArrayList<>();
    tsIt.forEach((ts) -> dest.add(toSparkTimestamp(ts)));
    return new GenericArrayData(dest.toArray(new Long[0]));
  }

  public static GenericArrayData zonedDateTimeIterToSparkDates(Iterable<ZonedDateTime> tsIt) {
    List<Integer> dest = new ArrayList<>();
    tsIt.forEach((ts) -> dest.add(zonedDateTimeToSparkDate(ts)));
    return new GenericArrayData(dest.toArray(new Integer[0]));
  }

  public static GenericArrayData zonedDateTimeIterToSparkTimestamps(Iterable<ZonedDateTime> tsIt) {
    List<Long> dest = new ArrayList<>();
    tsIt.forEach((ts) -> dest.add(zonedDateTimeToSparkTimestamp(ts)));
    return new GenericArrayData(dest.toArray(new Long[0]));
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
          sparkRow.update(i, toSparkDate(spannerRow.getDate(i)));
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
          // Convert the timestamp to microseconds, which is supported in the Spark.
          sparkRow.update(i, toSparkTimestamp(spannerRow.getTimestamp(i)));
          break;

        case STRING:
          sparkRow.update(i, UTF8String.fromString(spannerRow.getString(i)));
          break;

        case BYTES:
          sparkRow.update(i, spannerRow.getBytes(i).toByteArray());
          break;

        case STRUCT:
          sparkRow.update(i, spannerStructToInternalRow(spannerRow.getStruct(i)));
          break;

        default: // "ARRAY"
          String fieldTypeName = spannerRow.getColumnType(i).toString();
          // Note: for ARRAY<T,...>, T MUST be the homogenous (same type) within the ARRAY, per:
          // https://cloud.google.com/spanner/docs/reference/standard-sql/data-types#array_type

          // It is imperative that we use .getValue(i) instead of directly invoking
          // spannerRow.get<T>Array(i) that we invoke spannerRow.getValue(i).get<T>Array()
          // otherwise if the array contains any NULL values such as in:
          //    [1, 2, NULL, 4, 5]
          // that Cloud Spanner's Java library will panic due to its checkNotNull checks.
          // Please see https://github.com/GoogleCloudDataproc/spark-spanner-connector/issues/95
          Value value = spannerRow.getValue(i);

          if (fieldTypeName.indexOf("ARRAY<BOOL>") == 0) {
            sparkRow.update(i, new GenericArrayData(value.getBoolArray().toArray(new Boolean[0])));
          } else if (fieldTypeName.indexOf("ARRAY<FLOAT64>") == 0) {
            sparkRow.update(
                i, new GenericArrayData(value.getFloat64Array().toArray(new Double[0])));
          } else if (fieldTypeName.indexOf("ARRAY<INT64>") == 0) {
            if (value.isNull()) {
              sparkRow.update(i, null);
            } else {
              List<Long> i64L = value.getInt64Array();
              sparkRow.update(i, new GenericArrayData(i64L.toArray(new Long[0])));
            }
          } else if (fieldTypeName.indexOf("ARRAY<STRING") == 0) {
            List<String> src = value.getStringArray();
            List<UTF8String> dest = new ArrayList<UTF8String>(src.size());
            src.forEach((s) -> dest.add(UTF8String.fromString(s)));
            sparkRow.update(i, new GenericArrayData(dest.toArray(new UTF8String[0])));
          } else if (fieldTypeName.indexOf("ARRAY<TIMESTAMP>") == 0) {
            List<Long> endTsL = new ArrayList<>();
            value.getTimestampArray().forEach((ts) -> endTsL.add(toSparkTimestamp(ts)));
            sparkRow.update(i, new GenericArrayData(endTsL.toArray(new Long[0])));
          } else if (fieldTypeName.indexOf("ARRAY<DATE>") == 0) {
            List<Integer> endDL = new ArrayList<>();
            value.getDateArray().forEach((ts) -> endDL.add(toSparkDate(ts)));
            sparkRow.update(i, new GenericArrayData(endDL.toArray(new Integer[0])));
          } else if (fieldTypeName.indexOf("ARRAY<JSON") == 0) {
            List<String> src = value.getJsonArray();
            List<UTF8String> dest = new ArrayList<UTF8String>(src.size());
            src.forEach((s) -> dest.add(UTF8String.fromString(s)));
            sparkRow.update(i, new GenericArrayData(dest.toArray(new UTF8String[0])));
          } else if (fieldTypeName.indexOf("ARRAY<BYTES") == 0) {
            List<ByteArray> src = value.getBytesArray();
            byte[][] byteArray = new byte[src.size()][];
            List<byte[]> dest = new ArrayList<byte[]>(src.size());
            int it = 0;
            for (ByteArray bytes : src) {
              byteArray[it++] = bytes == null ? null : bytes.toByteArray();
            }
            sparkRow.update(i, new GenericArrayData(byteArray));
          } else if (fieldTypeName.indexOf("ARRAY<STRUCT<") == 0) {
            List<InternalRow> dest = new ArrayList<>();
            value.getStructArray().forEach((st) -> dest.add(spannerStructToInternalRow(st)));
            sparkRow.update(i, new GenericArrayData(dest.toArray(new InternalRow[0])));
          } else if (fieldTypeName.indexOf("ARRAY<NUMERIC>") == 0) {
            List<Decimal> dest = new ArrayList<>();
            value.getNumericArray().forEach((v) -> dest.add(asSparkDecimal(v)));
            sparkRow.update(i, new GenericArrayData(dest.toArray(new Decimal[0])));
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
