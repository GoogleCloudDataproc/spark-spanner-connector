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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.cloud.ByteArray;
import com.google.cloud.spanner.*;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.ConnectionOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.CharStreams;
import com.google.common.io.Closeables;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import javax.annotation.Nullable;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.threeten.bp.Duration;

public class SpannerUtils {

  private static final RetrySettings RETRY_SETTING =
      RetrySettings.newBuilder()
          .setInitialRpcTimeout(Duration.ofHours(2))
          .setMaxRpcTimeout(Duration.ofHours(2))
          .setTotalTimeout(Duration.ofHours(2))
          .setRpcTimeoutMultiplier(1.0)
          .setInitialRetryDelay(Duration.ofSeconds(2))
          .setMaxRetryDelay(Duration.ofSeconds(60))
          .setRetryDelayMultiplier(1.5)
          .setMaxAttempts(100)
          .build();
  private static final ObjectMapper jsonMapper = new ObjectMapper();

  @VisibleForTesting
  static String GCP_REGION_PART = getGcpRegion().map(region -> " region/" + region).orElse("");

  @VisibleForTesting
  static String DATAPROC_IMAGE_PART =
      Optional.ofNullable(System.getenv("DATAPROC_IMAGE_VERSION"))
          .map(image -> "dataproc-image/" + image)
          .orElse("");

  static final String CONNECTOR_VERSION = loadConnectorVersion();

  public static final String COLUMN_TYPE = "col_type";

  public static Long SECOND_TO_DAYS = 60 * 60 * 24L;

  private static final String SPARK_VERSION = org.apache.spark.package$.MODULE$.SPARK_VERSION();
  private static final String JAVA_VERSION = System.getProperty("java.runtime.version");
  private static final String SCALA_VERSION = scala.util.Properties.versionNumberString();

  private static final String USER_AGENT =
      String.format(
          "spark-spanner/%s spark/%s java/%s scala/%s %s %s",
          CONNECTOR_VERSION,
          SPARK_VERSION,
          JAVA_VERSION,
          SCALA_VERSION,
          GCP_REGION_PART,
          DATAPROC_IMAGE_PART);
  private static final SessionPoolOptions defaultSessionPoolOptions =
      SessionPoolOptions.newBuilder().build();

  private static String loadConnectorVersion() {
    try {
      Properties buildProperties = new Properties();
      InputStream inputStream =
          SpannerUtils.class.getResourceAsStream("/spark-spanner-connector.properties");
      if (inputStream == null) {
        // Failed to fetch the Spark Spanner connector version.
        return "";
      }
      buildProperties.load(inputStream);
      return buildProperties.getProperty("connector.version");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  // Queries the GCE metadata server
  @VisibleForTesting
  static Optional<String> getGcpRegion() {
    RequestConfig config =
        RequestConfig.custom()
            .setConnectTimeout(100)
            .setConnectionRequestTimeout(100)
            .setSocketTimeout(100)
            .build();
    CloseableHttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(config).build();
    HttpGet httpGet =
        new HttpGet("http://metadata.google.internal/computeMetadata/v1/instance/zone");
    httpGet.addHeader("Metadata-Flavor", "Google");
    try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
      if (response.getStatusLine().getStatusCode() == 200) {
        String body =
            CharStreams.toString(new InputStreamReader(response.getEntity().getContent(), UTF_8));
        return Optional.of(body.substring(body.lastIndexOf('/') + 1));
      } else {
        return Optional.empty();
      }
    } catch (Exception e) {
      return Optional.empty();
    } finally {
      try {
        Closeables.close(httpClient, true);
      } catch (IOException e) {
        // nothing to do
      }
    }
  }

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
    return batchClientFromProperties(properties, defaultSessionPoolOptions);
  }

  public static BatchClientWithCloser batchClientFromProperties(
      Map<String, String> properties, SessionPoolOptions sessionPoolOptions) {
    SpannerOptions.Builder builder =
        SpannerOptions.newBuilder()
            .setSessionPoolOption(sessionPoolOptions)
            .setProjectId(properties.get("projectId"))
            .setHeaderProvider(FixedHeaderProvider.create("user-agent", USER_AGENT));
    builder
        .getSpannerStubSettingsBuilder()
        .executeStreamingSqlSettings()
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
    System.setProperty("com.google.cloud.spanner.watchdogTimeoutSeconds", "7200");

    SpannerOptions options = builder.build();
    Spanner spanner = options.getService();
    DatabaseId databaseId =
        DatabaseId.of(
            options.getProjectId(), properties.get("instanceId"), properties.get("databaseId"));
    return new BatchClientWithCloser(
        spanner, spanner.getBatchClient(databaseId), spanner.getDatabaseClient(databaseId));
  }

  public static void toSparkDecimal(GenericInternalRow dest, java.math.BigDecimal v, int at) {
    if (v == null) {
      dest.update(at, null);
      return;
    }
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
    try {
      dec.set(new scala.math.BigDecimal(v), 38, 9);
    } catch (ArithmeticException e) {
      if (e.getMessage().contains("exceeds max precision")) {
        throw new SpannerConnectorException(
            SpannerErrorCode.DECIMAL_OUT_OF_RANGE,
            "The spannner DB may contain Decimal type that is out of scope:" + e.getMessage());
      }
      throw e;
    }
    return dec;
  }

  private static void spannerNumericToSpark(
      Struct src, int srcIndex, GenericInternalRow dest, int destIndex) {
    toSparkDecimal(dest, src.getBigDecimal(srcIndex), destIndex);
  }

  private static void spannerNumericToSparkPg(
      Struct src, int srcIndex, GenericInternalRow dest, int destIndex) {
    toSparkDecimal(
        dest,
        Double.isNaN(src.getValue(srcIndex).getFloat64())
            ? null
            : src.getValue(srcIndex).getNumeric(),
        destIndex);
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
    // ts.getTime() returns time in milliseconds, so * 1000 -> microseconds
    // ts.getNanos() returns time in nanoseconds, (so / 1000) % 1000 -> microseconds
    return (ts.getTime() * 1000) + ((ts.getNanos() / 1000) % 1000);
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

  private static final ZoneId zoneUTC = ZoneId.of("UTC+00:00");

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

  public static void convertRowAt(
      Struct spannerRow, int spannerRowIndex, GenericInternalRow sparkRow, int sparkRowIndex) {
    if (spannerRow.isNull(spannerRowIndex)) {
      sparkRow.update(sparkRowIndex, null);
      return;
    }

    Type type = spannerRow.getColumnType(spannerRowIndex);
    switch (type.getCode()) {
      case BOOL:
        sparkRow.setBoolean(sparkRowIndex, spannerRow.getBoolean(spannerRowIndex));
        break;

      case DATE:
        sparkRow.update(sparkRowIndex, toSparkDate(spannerRow.getDate(spannerRowIndex)));
        break;

      case FLOAT64:
        sparkRow.setDouble(sparkRowIndex, spannerRow.getDouble(spannerRowIndex));
        break;

      case INT64:
        sparkRow.setLong(sparkRowIndex, spannerRow.getLong(spannerRowIndex));
        break;

      case JSON:
        sparkRow.update(sparkRowIndex, UTF8String.fromString(spannerRow.getJson(spannerRowIndex)));
        break;

      case PG_JSONB:
        sparkRow.update(
            sparkRowIndex, UTF8String.fromString(spannerRow.getPgJsonb(spannerRowIndex)));
        break;

      case NUMERIC:
        spannerNumericToSpark(spannerRow, spannerRowIndex, sparkRow, sparkRowIndex);
        break;

      case PG_NUMERIC:
        spannerNumericToSparkPg(spannerRow, spannerRowIndex, sparkRow, sparkRowIndex);
        break;

      case TIMESTAMP:
        // Convert the timestamp to microseconds, which is supported in the Spark.
        sparkRow.update(sparkRowIndex, toSparkTimestamp(spannerRow.getTimestamp(spannerRowIndex)));
        break;

      case STRING:
        sparkRow.update(
            sparkRowIndex, UTF8String.fromString(spannerRow.getString(spannerRowIndex)));
        break;

      case BYTES:
        sparkRow.update(sparkRowIndex, spannerRow.getBytes(spannerRowIndex).toByteArray());
        break;

      case STRUCT:
        sparkRow.update(
            sparkRowIndex, spannerStructToInternalRow(spannerRow.getStruct(spannerRowIndex)));
        break;

      case ARRAY:
        // TODO: Avoid to string here and use code enum instead
        String fieldTypeName = spannerRow.getColumnType(spannerRowIndex).toString();
        // Note: for ARRAY<T,...>, T MUST be the homogenous (same type) within the ARRAY, per:
        // https://cloud.google.com/spanner/docs/reference/standard-sql/data-types#array_type

        // It is imperative that we use .getValue(i) instead of directly invoking
        // spannerRow.get<T>Array(i) that we invoke spannerRow.getValue(i).get<T>Array()
        // otherwise if the array contains any NULL values such as in:
        //    [1, 2, NULL, 4, 5]
        // that Cloud Spanner's Java library will panic due to its checkNotNull checks.
        // Please see https://github.com/GoogleCloudDataproc/spark-spanner-connector/issues/95
        Value value = spannerRow.getValue(spannerRowIndex);

        if (fieldTypeName.indexOf("ARRAY<BOOL>") == 0) {
          sparkRow.update(
              sparkRowIndex, new GenericArrayData(value.getBoolArray().toArray(new Boolean[0])));
        } else if (fieldTypeName.indexOf("ARRAY<FLOAT64>") == 0) {
          sparkRow.update(
              sparkRowIndex, new GenericArrayData(value.getFloat64Array().toArray(new Double[0])));
        } else if (fieldTypeName.indexOf("ARRAY<INT64>") == 0) {
          if (value.isNull()) {
            sparkRow.update(sparkRowIndex, null);
          } else {
            List<Long> i64L = value.getInt64Array();
            sparkRow.update(sparkRowIndex, new GenericArrayData(i64L.toArray(new Long[0])));
          }
        } else if (fieldTypeName.indexOf("ARRAY<STRING") == 0) {
          List<String> src = value.getStringArray();
          List<UTF8String> dest = new ArrayList<>(src.size());
          src.forEach((s) -> dest.add(UTF8String.fromString(s)));
          sparkRow.update(sparkRowIndex, new GenericArrayData(dest.toArray(new UTF8String[0])));
        } else if (fieldTypeName.indexOf("ARRAY<TIMESTAMP>") == 0) {
          List<Long> endTsL = new ArrayList<>();
          value.getTimestampArray().forEach((ts) -> endTsL.add(toSparkTimestamp(ts)));
          sparkRow.update(sparkRowIndex, new GenericArrayData(endTsL.toArray(new Long[0])));
        } else if (fieldTypeName.indexOf("ARRAY<DATE>") == 0) {
          List<Integer> endDL = new ArrayList<>();
          value.getDateArray().forEach((ts) -> endDL.add(toSparkDate(ts)));
          sparkRow.update(sparkRowIndex, new GenericArrayData(endDL.toArray(new Integer[0])));
        } else if (fieldTypeName.indexOf("ARRAY<JSON<PG_JSONB") == 0) {
          List<String> src = value.getPgJsonbArray();
          List<UTF8String> dest = new ArrayList<>(src.size());
          src.forEach((s) -> dest.add(UTF8String.fromString(s)));
          sparkRow.update(sparkRowIndex, new GenericArrayData(dest.toArray(new UTF8String[0])));
        } else if (fieldTypeName.indexOf("ARRAY<JSON") == 0) {
          List<String> src = value.getJsonArray();
          List<UTF8String> dest = new ArrayList<>(src.size());
          src.forEach((s) -> dest.add(UTF8String.fromString(s)));
          sparkRow.update(sparkRowIndex, new GenericArrayData(dest.toArray(new UTF8String[0])));
        } else if (fieldTypeName.indexOf("ARRAY<BYTES") == 0) {
          List<ByteArray> src = value.getBytesArray();
          byte[][] byteArray = new byte[src.size()][];
          int it = 0;
          for (ByteArray bytes : src) {
            byteArray[it++] = bytes == null ? null : bytes.toByteArray();
          }
          sparkRow.update(sparkRowIndex, new GenericArrayData(byteArray));
        } else if (fieldTypeName.indexOf("ARRAY<STRUCT<") == 0) {
          List<InternalRow> dest = new ArrayList<>();
          value.getStructArray().forEach((st) -> dest.add(spannerStructToInternalRow(st)));
          sparkRow.update(sparkRowIndex, new GenericArrayData(dest.toArray(new InternalRow[0])));
        } else if (fieldTypeName.indexOf("ARRAY<NUMERIC>") == 0
            || fieldTypeName.indexOf("ARRAY<NUMERIC<PG_NUMERIC>>") == 0) {
          List<Decimal> dest = new ArrayList<>();
          value.getNumericArray().forEach((v) -> dest.add(asSparkDecimal(v)));
          sparkRow.update(sparkRowIndex, new GenericArrayData(dest.toArray(new Decimal[0])));
        } else {
          sparkRow.update(sparkRowIndex, null);
        }
        break;

      default:
        throw new SpannerConnectorException(
            SpannerErrorCode.UNSUPPORTED,
            String.format(
                "Spanner column type %s is not supported. spannerRowIndex=%d",
                type, spannerRowIndex));
    }
  }

  public static InternalRow spannerStructToInternalRow(Struct spannerRow) {
    int columnCount = spannerRow.getColumnCount();
    GenericInternalRow sparkRow = new GenericInternalRow(columnCount);

    for (int i = 0; i < columnCount; i++) {
      convertRowAt(spannerRow, i, sparkRow, i);
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

  public static StructType pruneSchema(
      StructType originalSchema, @Nullable Set<String> includeColumns) {
    if (includeColumns == null) return originalSchema;
    StructType prunedSchema = new StructType();
    for (StructField field : originalSchema.fields()) {
      if (includeColumns.contains(field.name())) {
        prunedSchema = prunedSchema.add(field);
      }
    }
    return prunedSchema;
  }

    static String getRequiredOption(Map<String, String> properties, String option) {
      return Objects.requireNonNull(properties.get(option), "Option \"" + option + "\" is missing.");
    }
}
